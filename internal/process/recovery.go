package process

import (
	"context"
	"errors"
	"time"

	"github.com/wyw14/cry-97/internal/dosing"
	"github.com/wyw14/cry-97/internal/intake"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

// RecoverLine rebuilds the in-memory state of one line from its committed
// event stream. The journal is the source of truth: every committed event is
// projected so batches, stage, generation and resource occupancy match what
// was persisted. A snapshot is only a checkpoint; it is adopted when the
// journal is shorter than the checkpoint (truncated) and otherwise refreshed
// from the projected result.
//
// Projection is deterministic and idempotent, so RecoverLine may run more
// than once without re-dispatching device commands.
func (p *Plant) RecoverLine(ctx context.Context, lineID model.LineID) (uint64, error) {
	snapshot, snapshotFound, err := p.snapshots.Load(lineID)
	if err != nil {
		return 0, err
	}

	// Always project from a clean baseline so a terminal snapshot cannot
	// suppress earlier events that still have to rebuild the registries, and
	// so a repeated recovery derives exactly the event stream with no stale
	// batch or resource left behind.
	p.resetLineProjection(lineID)

	replayer, err := journal.NewReplayer(p.store, p)
	if err != nil {
		return 0, err
	}
	last, err := replayer.ReplayLine(ctx, lineID)
	if err != nil {
		return last, err
	}

	p.mu.Lock()
	state := p.lines[lineID]
	p.mu.Unlock()

	// The journal was truncated behind the checkpoint: trust the snapshot's
	// terminal line state, which no longer has backing events.
	if snapshotFound && snapshot.LastEventSeq > last {
		state = snapshot
		p.mu.Lock()
		p.lines[lineID] = state
		p.mu.Unlock()
		last = snapshot.LastEventSeq
	}

	if err := p.snapshots.Save(state); err != nil {
		return last, err
	}
	return last, nil
}

func (p *Plant) ApplyRecovered(ctx context.Context, event model.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	state, ok := p.lines[event.LineID]
	p.mu.Unlock()
	if !ok {
		return errors.New("recovery line is not configured")
	}
	if event.Sequence <= state.LastEventSeq {
		return nil
	}
	// Once the line has entered a newer generation, events from a superseded
	// generation must not resurrect old state. They are still committed, so
	// advance the watermark past them.
	if state.Emergency && event.Generation < state.Generation {
		p.advanceWatermark(event.LineID, event.Sequence, event.OccurredAt)
		return nil
	}

	if event.IsTerminal() {
		if err := p.applyEmergency(event); err != nil {
			return err
		}
		return nil
	}

	switch event.Kind {
	case model.EventBatchCreated:
		if err := p.applyBatchCreated(event); err != nil {
			return err
		}
	case model.EventStageChanged:
		if err := p.applyStageChanged(event); err != nil {
			return err
		}
	case model.EventDosingChanged:
		var change dosing.Change
		if err := event.Decode(&change); err != nil {
			return err
		}
		p.dosing.Restore(change)
	case model.EventDeviceCommanded:
		var command model.DeviceCommand
		if err := event.Decode(&command); err != nil {
			return err
		}
		// Rebuilds last-known device state only; deduped by command ID so a
		// repeated recovery never dispatches the command again.
		p.devices.Restore(command)
	case model.EventSampleStored:
		// Sample progress lives in the persisted cursor; nothing else to fold.
	}

	p.advanceWatermark(event.LineID, event.Sequence, event.OccurredAt)
	return nil
}

func (p *Plant) applyBatchCreated(event model.Event) error {
	var started model.BatchStarted
	if err := event.Decode(&started); err != nil {
		// Backward compatibility with journals that stored the bare batch.
		var batch model.Batch
		if err := event.Decode(&batch); err != nil {
			return err
		}
		started = model.BatchStarted{Batch: batch}
	}
	batch := started.Batch
	if batch.LineID != event.LineID {
		return errors.New("batch created payload does not match event line")
	}
	p.mu.Lock()
	p.batches[batch.ID] = batch
	p.lineBatches[event.LineID] = batch.ID
	state := p.lines[event.LineID]
	state.Stage = batch.Stage
	state.BatchID = batch.ID.String()
	if batch.Generation > state.Generation {
		state.Generation = batch.Generation
	}
	state.UpdatedAt = event.OccurredAt
	p.lines[event.LineID] = state
	p.mu.Unlock()

	if started.Source != "" {
		p.intake.Restore(intake.Admission{
			LineID:      event.LineID,
			FlowM3Hour:  started.FlowM3Hour,
			Source:      started.Source,
			AcceptedAt:  admissionsTime(started, event),
			EqualizerID: started.EqualizerID,
		})
	}
	if started.EqualizerID != "" {
		p.equalizers.Restore(started.EqualizerID, event.LineID, started.FlowM3Hour, admissionsTime(started, event))
	}
	return nil
}

func (p *Plant) applyStageChanged(event model.Event) error {
	var batch model.Batch
	if err := event.Decode(&batch); err != nil {
		return err
	}
	if batch.LineID != event.LineID {
		return errors.New("stage changed payload does not match event line")
	}
	p.mu.Lock()
	p.batches[batch.ID] = batch
	p.lineBatches[event.LineID] = batch.ID
	state := p.lines[event.LineID]
	state.Stage = batch.Stage
	state.BatchID = batch.ID.String()
	if batch.Generation > state.Generation {
		state.Generation = batch.Generation
	}
	state.UpdatedAt = event.OccurredAt
	p.lines[event.LineID] = state
	p.mu.Unlock()
	return nil
}

func (p *Plant) applyEmergency(event model.Event) error {
	var record EmergencyRecord
	if err := event.Decode(&record); err != nil {
		return err
	}
	if record.LineID != event.LineID || record.Generation != event.Generation {
		return errors.New("emergency recovery payload does not match event identity")
	}
	p.mu.Lock()
	state := p.lines[event.LineID].Isolate(event.Sequence, event.OccurredAt)
	p.lines[event.LineID] = state
	p.mu.Unlock()
	// Idempotent side effects: re-running recovery reaches the same state.
	p.devices.StopLine(event.LineID, state.Generation)
	p.equalizers.Release(event.LineID)
	return nil
}

func (p *Plant) advanceWatermark(lineID model.LineID, sequence uint64, at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.lines[lineID]
	if sequence > state.LastEventSeq {
		state.LastEventSeq = sequence
	}
	state.UpdatedAt = at.UTC()
	p.lines[lineID] = state
}

func admissionsTime(started model.BatchStarted, event model.Event) time.Time {
	if !started.AcceptedAt.IsZero() {
		return started.AcceptedAt
	}
	return event.OccurredAt
}

// resetLineProjection clears every in-memory structure derived from a line's
// event stream so the following replay reconstructs state solely from
// committed events.
func (p *Plant) resetLineProjection(lineID model.LineID) {
	p.mu.Lock()
	for batchID, batch := range p.batches {
		if batch.LineID == lineID {
			delete(p.batches, batchID)
		}
	}
	delete(p.lineBatches, lineID)
	p.lines[lineID] = model.NewLineState(lineID, p.now())
	p.mu.Unlock()

	p.intake.Clear(lineID)
	p.equalizers.Release(lineID)
	p.dosing.ClearLine(lineID)
	p.devices.ClearLine(lineID)
}
