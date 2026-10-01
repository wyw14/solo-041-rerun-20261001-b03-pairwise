package dosing

import (
	"errors"
	"sort"
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

// Restore re-adds a change captured by a committed event during replay.
// It is idempotent by change ID and keeps per-batch history ordered.
func (l *Ledger) Restore(change Change) {
	l.mu.Lock()
	defer l.mu.Unlock()
	values := l.changes[change.BatchID]
	for _, existing := range values {
		if existing.ID == change.ID {
			return
		}
	}
	values = append(values, change)
	sort.Slice(values, func(i, j int) bool { return values[i].RequestedAt.Before(values[j].RequestedAt) })
	l.changes[change.BatchID] = values
}

// ClearLine drops restored changes belonging to a line before re-projection.
func (l *Ledger) ClearLine(lineID model.LineID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for batchID, values := range l.changes {
		kept := values[:0]
		for _, change := range values {
			if change.LineID != lineID {
				kept = append(kept, change)
			}
		}
		if len(kept) == 0 {
			delete(l.changes, batchID)
		} else {
			l.changes[batchID] = kept
		}
	}
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
