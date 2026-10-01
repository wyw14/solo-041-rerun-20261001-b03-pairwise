package sensor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

type Cursor interface {
	Current(model.LineID) uint64
	Commit(model.LineID, uint64) error
}

type Receiver struct {
	journal journal.Appender
	cursor  Cursor
}

func NewReceiver(appender journal.Appender, cursor Cursor) (*Receiver, error) {
	if appender == nil || cursor == nil {
		return nil, errors.New("sample receiver requires journal and cursor")
	}
	return &Receiver{journal: appender, cursor: cursor}, nil
}

func (r *Receiver) Receive(ctx context.Context, sample model.Sample, now time.Time) (model.Event, error) {
	if !sample.Finite() || sample.Sequence == 0 || sample.LineID == "" {
		return model.Event{}, errors.New("sample is invalid")
	}
	current := r.cursor.Current(sample.LineID)
	if sample.Sequence <= current {
		return model.Event{}, nil
	}
	if sample.Sequence != current+1 {
		return model.Event{}, fmt.Errorf("sample sequence gap: current=%d received=%d", current, sample.Sequence)
	}
	event, err := model.NewEvent(sample.LineID, 1, model.EventSampleStored, sample, now)
	if err != nil {
		return model.Event{}, err
	}
	persisted, err := r.journal.Append(ctx, event)
	if err != nil {
		return model.Event{}, fmt.Errorf("store sample %d failed: %w", sample.Sequence, err)
	}
	if err := r.cursor.Commit(sample.LineID, sample.Sequence); err != nil {
		return model.Event{}, fmt.Errorf("commit sample cursor: %w", err)
	}
	return persisted, nil
}

func (r *Receiver) Current(lineID model.LineID) uint64 {
	return r.cursor.Current(lineID)
}

// CommitSequence advances the durable sample cursor during recovery. The
// cursor is a pure checkpoint that only moves forward, so committing a
// sequence already present in the journal is safe and closes gaps left by a
// crash between event append and cursor commit.
func (r *Receiver) CommitSequence(lineID model.LineID, sequence uint64) error {
	if lineID == "" || sequence == 0 {
		return errors.New("sample cursor identity is incomplete")
	}
	return r.cursor.Commit(lineID, sequence)
}
