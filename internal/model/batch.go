package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Batch struct {
	ID                  uuid.UUID    `json:"id"`
	LineID              LineID       `json:"line_id"`
	Stage               ProcessStage `json:"stage"`
	Generation          uint64       `json:"generation"`
	Qualified           bool         `json:"qualified"`
	QualificationSample uuid.UUID    `json:"qualification_sample,omitempty"`
	QualificationRev    uint64       `json:"qualification_revision,omitempty"`
	CreatedAt           time.Time    `json:"created_at"`
	UpdatedAt           time.Time    `json:"updated_at"`
}

func NewBatch(lineID LineID, now time.Time) Batch {
	stamp := now.UTC()
	return Batch{
		ID: uuid.New(), LineID: lineID, Stage: StageReceiving,
		Generation: 1, CreatedAt: stamp, UpdatedAt: stamp,
	}
}

// BatchStarted is the payload of EventBatchCreated. It carries the intake
// admission so that resource occupancy can be rebuilt during replay.
type BatchStarted struct {
	Batch       Batch     `json:"batch"`
	FlowM3Hour  float64   `json:"flow_m3_hour,omitempty"`
	Source      string    `json:"source,omitempty"`
	EqualizerID string    `json:"equalizer_id,omitempty"`
	AcceptedAt  time.Time `json:"accepted_at,omitempty"`
}

func (b Batch) Move(next ProcessStage, now time.Time) (Batch, error) {
	state := LineState{ID: b.LineID, Stage: b.Stage, Generation: b.Generation}
	advanced, err := state.Advance(next, now)
	if err != nil {
		return b, err
	}
	b.Stage = advanced.Stage
	b.UpdatedAt = advanced.UpdatedAt
	return b, nil
}

func (b Batch) ApplyQualification(result LabResult, now time.Time) (Batch, error) {
	if !result.Valid {
		return b, errors.New("lab result is not valid")
	}
	if result.SampleID == uuid.Nil || result.Revision == 0 {
		return b, errors.New("lab result identity is incomplete")
	}
	if b.QualificationSample == result.SampleID && result.Revision <= b.QualificationRev {
		return b, errors.New("lab result revision is stale")
	}
	b.QualificationSample = result.SampleID
	b.QualificationRev = result.Revision
	b.Qualified = result.Qualified()
	b.UpdatedAt = now.UTC()
	if b.Qualified && b.Stage == StageSettling {
		b.Stage = StageQualified
	}
	if !b.Qualified && b.Stage == StageQualified {
		b.Stage = StageSettling
	}
	return b, nil
}

func (b Batch) IsActive() bool {
	return b.Stage != StageClosed && b.Stage != StageIsolated
}
