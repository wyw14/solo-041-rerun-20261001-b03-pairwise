package process

import (
	"context"
	"errors"

	"github.com/wyw14/cry-97/internal/model"
)

type EmergencyRecord struct {
	LineID     model.LineID `json:"line_id"`
	Reason     string       `json:"reason"`
	Generation uint64       `json:"generation"`
}

func (p *Plant) EmergencyStop(ctx context.Context, lineID model.LineID, reason string) (model.LineState, error) {
	if reason == "" {
		return model.LineState{}, errors.New("emergency reason is required")
	}
	p.recoveryMu.RLock()
	defer p.recoveryMu.RUnlock()
	p.mu.RLock()
	state, ok := p.lines[lineID]
	p.mu.RUnlock()
	if !ok {
		return model.LineState{}, errors.New("process line is not found")
	}
	now := p.now()
	record := EmergencyRecord{LineID: lineID, Reason: reason, Generation: state.Generation + 1}
	event, err := model.NewEvent(lineID, record.Generation, model.EventEmergencyIsolated, record, now)
	if err != nil {
		return model.LineState{}, err
	}
	persisted, err := p.store.Append(ctx, event)
	if err != nil {
		return model.LineState{}, err
	}
	p.mu.Lock()
	state = p.lines[lineID].Isolate(persisted.Sequence, now)
	p.lines[lineID] = state
	if batchID, hasBatch := p.lineBatches[lineID]; hasBatch {
		batch := p.batches[batchID]
		batch.Stage = model.StageIsolated
		batch.UpdatedAt = now
		p.batches[batchID] = batch
	}
	p.mu.Unlock()
	p.devices.StopLine(lineID, state.Generation)
	p.releaseIntake(lineID)
	p.alarms.Raise(model.NewAlarm(lineID, "PROCESS_EMERGENCY", reason, model.SeverityCritical, now))
	if err := p.persistLineSnapshot(lineID); err != nil {
		return model.LineState{}, err
	}
	return state, nil
}
