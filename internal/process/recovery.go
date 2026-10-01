package process

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/dosing"
	"github.com/wyw14/cry-97/internal/intake"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

// lineSnapshot is the persisted projection of one line at LastEventSeq. The
// journal remains the source of truth; the snapshot only lets recovery skip
// events that have already been folded.
type lineSnapshot struct {
	Line         model.LineState            `json:"line"`
	Batches      []model.Batch              `json:"batches,omitempty"`
	CurrentBatch *uuid.UUID                 `json:"current_batch,omitempty"`
	Admission    *intake.Admission          `json:"admission,omitempty"`
	Doses        map[string][]dosing.Change `json:"doses,omitempty"`
	Commands     []model.DeviceCommand      `json:"commands,omitempty"`
	LastEventSeq uint64                     `json:"last_event_seq"`
}

// RecoverLine rebuilds a line's in-memory state so it matches its committed
// event log. It installs the last snapshot (if any), folds every event
// committed after it, and then refreshes the snapshot. Device commands are
// only restored into the command projection; they are never dispatched, and
// running recovery again yields the same state without duplicate commands.
func (p *Plant) RecoverLine(ctx context.Context, lineID model.LineID) (uint64, error) {
	// Hold the recovery lock for the whole rebuild. Live mutations take the
	// read side, so they cannot interleave with a replay; concurrent
	// recoveries simply serialize and converge on the same projection.
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()

	p.mu.RLock()
	_, configured := p.lines[lineID]
	p.mu.RUnlock()
	if !configured {
		return 0, errors.New("recovery line is not configured")
	}

	// Reset every line-scoped projection first so a repeated recovery cannot
	// double-apply anything, then install the snapshot as a baseline.
	p.resetLineProjection(lineID)

	var snap lineSnapshot
	found, err := p.snapshots.Load(lineID, &snap)
	if err != nil {
		return 0, err
	}
	var startAfter uint64
	if found && snap.Line.ID == lineID {
		if err := p.installSnapshot(lineID, snap); err != nil {
			return 0, err
		}
		startAfter = snap.LastEventSeq
	}

	events, err := p.store.EventsAfter(ctx, lineID, startAfter)
	if err != nil {
		return 0, err
	}
	ordered := journal.OrderByCommit(events)
	var sampleCursor uint64
	last := startAfter
	for _, event := range ordered {
		if err := ctx.Err(); err != nil {
			return last, err
		}
		if event.LineID != lineID {
			return last, fmt.Errorf("event %s belongs to line %s", event.ID, event.LineID)
		}
		if event.Sequence <= last {
			return last, errors.New("journal sequence is not strictly increasing")
		}
		sampleSeq, err := p.applyRecoveredEvent(event)
		if err != nil {
			return last, fmt.Errorf("recover event %s (seq %d): %w", event.ID, event.Sequence, err)
		}
		if sampleSeq > sampleCursor {
			sampleCursor = sampleSeq
		}
		last = event.Sequence
	}

	// The sample cursor is a separate monotonic checkpoint. Rebuild it from
	// sample events so a crash between event append and cursor commit cannot
	// leave a permanent gap.
	if sampleCursor > 0 {
		if err := p.receiver.CommitSequence(lineID, sampleCursor); err != nil {
			return last, err
		}
	}

	if err := p.persistLineSnapshot(lineID); err != nil {
		return last, err
	}
	return last, nil
}

// resetLineProjection returns all line-scoped in-memory state to its initial
// value. The sample cursor is left untouched because it only moves forward.
func (p *Plant) resetLineProjection(lineID model.LineID) {
	p.mu.Lock()
	p.lines[lineID] = model.NewLineState(lineID, p.now())
	delete(p.lineBatches, lineID)
	for id, batch := range p.batches {
		if batch.LineID == lineID {
			delete(p.batches, id)
		}
	}
	p.mu.Unlock()

	p.intake.Clear(lineID)
	p.equalizers.Release(lineID)
	p.dosing.ResetLine(lineID)
	p.devices.ResetLine(lineID)
	p.sampler.ResetLine(lineID)
}

func (p *Plant) installSnapshot(lineID model.LineID, snap lineSnapshot) error {
	p.mu.Lock()
	p.lines[lineID] = snap.Line
	var active *model.Batch
	for index := range snap.Batches {
		batch := snap.Batches[index]
		if batch.LineID != lineID {
			continue
		}
		p.batches[batch.ID] = batch
		if snap.CurrentBatch != nil && batch.ID == *snap.CurrentBatch {
			b := batch
			active = &b
		}
	}
	if snap.CurrentBatch != nil {
		p.lineBatches[lineID] = *snap.CurrentBatch
	}
	p.mu.Unlock()

	if snap.Admission != nil && active != nil && active.IsActive() {
		admission := *snap.Admission
		admission.LineID = lineID
		if err := p.intake.Restore(lineID, admission); err != nil {
			return err
		}
	}
	for _, changes := range snap.Doses {
		for _, change := range changes {
			p.dosing.Replay(change)
		}
	}
	for _, command := range snap.Commands {
		if command.LineID != lineID {
			continue
		}
		if err := p.devices.Restore(command); err != nil {
			return err
		}
	}
	return nil
}

// foldEffect lists subsystem projections that must be updated outside
// p.mu, keeping lock order intake -> plant (the order live admission takes).
type foldEffect struct {
	admit       *intake.Admission
	release     bool
	clearIntake bool
	doses       []dosing.Change
	commands    []model.DeviceCommand
	stopLine    bool
	sample      *model.Sample
}

// applyRecoveredEvent folds one committed event into in-memory state. It
// mutates projections only: no device is driven and no flow is admitted. It
// returns the highest recovered sample sequence (0 for other events).
func (p *Plant) applyRecoveredEvent(event model.Event) (uint64, error) {
	var effect foldEffect
	var sampleSeq uint64

	p.mu.Lock()
	state, ok := p.lines[event.LineID]
	if !ok {
		p.mu.Unlock()
		return 0, errors.New("recovery line is not configured")
	}

	switch event.Kind {
	case model.EventBatchCreated:
		var payload model.BatchCreated
		if err := event.Decode(&payload); err != nil {
			p.mu.Unlock()
			return 0, err
		}
		if payload.Batch.LineID == "" {
			// Journals written before the admission was embedded carry the
			// batch alone. Recover what the event still contains; the
			// equalizer assignment cannot be reconstructed from it.
			var legacy model.Batch
			if err := event.Decode(&legacy); err != nil {
				p.mu.Unlock()
				return 0, err
			}
			payload.Batch = legacy
		}
		batch := payload.Batch
		if batch.LineID != event.LineID || batch.Generation != event.Generation {
			p.mu.Unlock()
			return 0, errors.New("batch recovery payload does not match event identity")
		}
		p.batches[batch.ID] = batch
		p.lineBatches[event.LineID] = batch.ID
		state.BatchID = batch.ID.String()
		state.Stage = batch.Stage
		if batch.IsActive() && payload.Admission.EqualizerID != "" {
			effect.admit = &intake.Admission{
				LineID: event.LineID, FlowM3Hour: payload.Admission.FlowM3Hour,
				Source: payload.Admission.Source, EqualizerID: payload.Admission.EqualizerID,
				AcceptedAt: batch.CreatedAt,
			}
		}

	case model.EventStageChanged:
		var batch model.Batch
		if err := event.Decode(&batch); err != nil {
			p.mu.Unlock()
			return 0, err
		}
		if batch.LineID != event.LineID || batch.Generation != event.Generation {
			p.mu.Unlock()
			return 0, errors.New("stage recovery payload does not match event identity")
		}
		p.batches[batch.ID] = batch
		p.lineBatches[event.LineID] = batch.ID
		state.BatchID = batch.ID.String()
		state.Stage = batch.Stage
		if !batch.IsActive() {
			effect.clearIntake = true
			effect.release = true
		}

	case model.EventSampleStored:
		var sample model.Sample
		if err := event.Decode(&sample); err != nil {
			p.mu.Unlock()
			return 0, err
		}
		if sample.LineID != event.LineID {
			p.mu.Unlock()
			return 0, errors.New("sample recovery payload does not match event identity")
		}
		effect.sample = &sample
		sampleSeq = sample.Sequence

	case model.EventDosingChanged:
		var change dosing.Change
		if err := event.Decode(&change); err != nil {
			p.mu.Unlock()
			return 0, err
		}
		if change.LineID != event.LineID || change.Generation != event.Generation {
			p.mu.Unlock()
			return 0, errors.New("dosing recovery payload does not match event identity")
		}
		effect.doses = append(effect.doses, change)
		effect.commands = append(effect.commands, dosing.CommandFor(change))

	case model.EventDeviceCommanded:
		var command model.DeviceCommand
		if err := event.Decode(&command); err != nil {
			p.mu.Unlock()
			return 0, err
		}
		if command.LineID != event.LineID || command.Generation != event.Generation {
			p.mu.Unlock()
			return 0, errors.New("device recovery payload does not match event identity")
		}
		effect.commands = append(effect.commands, command)

	case model.EventEmergencyIsolated:
		var record EmergencyRecord
		if err := event.Decode(&record); err != nil {
			p.mu.Unlock()
			return 0, err
		}
		if record.LineID != event.LineID || record.Generation != event.Generation {
			p.mu.Unlock()
			return 0, errors.New("emergency recovery payload does not match event identity")
		}
		if batchID, hasBatch := p.lineBatches[event.LineID]; hasBatch {
			batch := p.batches[batchID]
			batch.Stage = model.StageIsolated
			batch.UpdatedAt = event.OccurredAt
			p.batches[batchID] = batch
		}
		state = state.Isolate(event.Sequence, event.OccurredAt)
		effect.clearIntake = true
		effect.release = true
		effect.stopLine = true

	default:
		p.mu.Unlock()
		return 0, fmt.Errorf("unknown recovered event kind %q", event.Kind)
	}

	state.LastEventSeq = event.Sequence
	state.UpdatedAt = event.OccurredAt
	p.lines[event.LineID] = state
	generation := state.Generation
	p.mu.Unlock()

	// Subsystem projections: rebuild only, never dispatch.
	if effect.admit != nil {
		if err := p.intake.Restore(event.LineID, *effect.admit); err != nil {
			return sampleSeq, err
		}
	}
	if effect.clearIntake {
		p.intake.Clear(event.LineID)
	}
	if effect.release {
		p.equalizers.Release(event.LineID)
	}
	for _, change := range effect.doses {
		p.dosing.Replay(change)
	}
	for _, command := range effect.commands {
		if err := p.devices.Restore(command); err != nil {
			return sampleSeq, err
		}
	}
	if effect.stopLine {
		p.devices.StopLine(event.LineID, generation)
	}
	if effect.sample != nil {
		p.sampler.Restore(*effect.sample)
	}
	return sampleSeq, nil
}

// persistLineSnapshot writes a projection of the line exactly as it stands
// after folding the committed log. Recovery is the only writer and runs one
// at a time per line, so snapshot and journal cannot disagree.
func (p *Plant) persistLineSnapshot(lineID model.LineID) error {
	snap := lineSnapshot{Doses: make(map[string][]dosing.Change)}

	p.mu.RLock()
	state, ok := p.lines[lineID]
	if !ok {
		p.mu.RUnlock()
		return errors.New("recovery line is not configured")
	}
	currentID, hasCurrent := p.lineBatches[lineID]
	if hasCurrent && currentID != uuid.Nil {
		snap.CurrentBatch = &currentID
	}
	for _, batch := range p.batches {
		if batch.LineID == lineID {
			snap.Batches = append(snap.Batches, batch)
		}
	}
	p.mu.RUnlock()
	sort.Slice(snap.Batches, func(i, j int) bool {
		return snap.Batches[i].CreatedAt.Before(snap.Batches[j].CreatedAt)
	})

	for _, batch := range snap.Batches {
		if history := p.dosing.History(batch.ID); len(history) > 0 {
			snap.Doses[batch.ID.String()] = history
		}
	}
	if admission, found := p.intake.Current(lineID); found {
		snap.Admission = &admission
	}
	snap.Commands = p.devices.ForLine(lineID)
	snap.Line = state
	snap.LastEventSeq = state.LastEventSeq
	return p.snapshots.Save(lineID, snap)
}
