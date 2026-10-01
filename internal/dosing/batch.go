package dosing

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type Change struct {
	ID          uuid.UUID    `json:"id"`
	LineID      model.LineID `json:"line_id"`
	BatchID     uuid.UUID    `json:"batch_id"`
	Chemical    string       `json:"chemical"`
	Rate        float64      `json:"rate"`
	Generation  uint64       `json:"generation"`
	RequestedAt time.Time    `json:"requested_at"`
}

func NewChange(lineID model.LineID, batchID uuid.UUID, chemical string, rate float64, generation uint64, now time.Time) (Change, error) {
	if lineID == "" || batchID == uuid.Nil || chemical == "" || rate <= 0 || generation == 0 {
		return Change{}, errors.New("dosing change is incomplete")
	}
	return Change{
		ID: uuid.New(), LineID: lineID, BatchID: batchID, Chemical: chemical,
		Rate: rate, Generation: generation, RequestedAt: now.UTC(),
	}, nil
}

type Ledger struct {
	mu      sync.RWMutex
	changes map[uuid.UUID][]Change
}

func NewLedger() *Ledger {
	return &Ledger{changes: make(map[uuid.UUID][]Change)}
}

func (l *Ledger) Record(change Change) {
	l.mu.Lock()
	l.changes[change.BatchID] = append(l.changes[change.BatchID], change)
	l.mu.Unlock()
}

func (l *Ledger) Latest(batchID uuid.UUID) (Change, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	values := l.changes[batchID]
	if len(values) == 0 {
		return Change{}, false
	}
	return values[len(values)-1], true
}

// Replay rebuilds the ledger from committed dosing changes. Later changes for
// the same change id replace the earlier entry so replay is idempotent.
func (l *Ledger) Replay(change Change) {
	l.mu.Lock()
	defer l.mu.Unlock()
	values := l.changes[change.BatchID]
	for index, existing := range values {
		if existing.ID == change.ID {
			values[index] = change
			l.changes[change.BatchID] = values
			return
		}
	}
	l.changes[change.BatchID] = append(values, change)
}

// History returns the committed changes for a batch in request order.
func (l *Ledger) History(batchID uuid.UUID) []Change {
	l.mu.RLock()
	defer l.mu.RUnlock()
	values := l.changes[batchID]
	if len(values) == 0 {
		return nil
	}
	return append([]Change(nil), values...)
}

// ResetLine drops every dosing change recorded against batches of the line.
func (l *Ledger) ResetLine(lineID model.LineID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for batchID, values := range l.changes {
		if len(values) > 0 && values[0].LineID == lineID {
			delete(l.changes, batchID)
		}
	}
}
