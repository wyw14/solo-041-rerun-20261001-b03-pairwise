package journal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/wyw14/cry-97/internal/model"
)

type Appender interface {
	Append(context.Context, model.Event) (model.Event, error)
}

type Reader interface {
	Events(context.Context, model.LineID) ([]model.Event, error)
}

type FileStore struct {
	dir       string
	mu        sync.Mutex
	sequences map[model.LineID]uint64
}

func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("journal directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	store := &FileStore{dir: dir, sequences: make(map[model.LineID]uint64)}
	if err := store.loadSequences(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *FileStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	if err := ctx.Err(); err != nil {
		return model.Event{}, err
	}
	if event.LineID == "" || event.ID.String() == "00000000-0000-0000-0000-000000000000" {
		return model.Event{}, errors.New("journal event identity is incomplete")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.sequences[event.LineID] + 1
	event.Sequence = next
	if err := appendEvent(s.linePath(event.LineID), event); err != nil {
		return model.Event{}, fmt.Errorf("append line %s event: %w", event.LineID, err)
	}
	s.sequences[event.LineID] = next
	return event, nil
}

func (s *FileStore) Events(ctx context.Context, lineID model.LineID) ([]model.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events, err := readEventFile(s.linePath(lineID))
	if err != nil {
		return nil, fmt.Errorf("read line %s events: %w", lineID, err)
	}
	return events, nil
}

// EventsAfter returns committed events with a sequence strictly greater than
// after, which is what replay needs once a snapshot at `after` is installed.
func (s *FileStore) EventsAfter(ctx context.Context, lineID model.LineID, after uint64) ([]model.Event, error) {
	events, err := s.Events(ctx, lineID)
	if err != nil {
		return nil, err
	}
	remaining := make([]model.Event, 0, len(events))
	for _, event := range events {
		if event.Sequence > after {
			remaining = append(remaining, event)
		}
	}
	return remaining, nil
}

func (s *FileStore) loadSequences() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
			continue
		}
		lineID := model.LineID(entry.Name()[:len(entry.Name())-len(".jsonl")])
		events, readErr := readEventFile(filepath.Join(s.dir, entry.Name()))
		if readErr != nil {
			return readErr
		}
		if len(events) > 0 {
			s.sequences[lineID] = events[len(events)-1].Sequence
		}
	}
	return nil
}

func (s *FileStore) linePath(lineID model.LineID) string {
	return filepath.Join(s.dir, string(lineID)+".jsonl")
}
