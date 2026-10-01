package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/wyw14/cry-97/internal/model"
)

type SnapshotStore struct {
	dir string
	mu  sync.Mutex
}

func NewSnapshotStore(dir string) (*SnapshotStore, error) {
	if dir == "" {
		return nil, errors.New("snapshot directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &SnapshotStore{dir: dir}, nil
}

// Save atomically persists a snapshot value for the line. The value must
// contain the committed event sequence it was built from; callers only save
// after events have been appended, so a snapshot can lag the journal but can
// never lead it.
func (s *SnapshotStore) Save(lineID model.LineID, value any) error {
	if lineID == "" {
		return errors.New("snapshot line is required")
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, string(lineID)+".json")
	temporary := path + ".next"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o640); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// Load decodes the line snapshot into target. A missing snapshot is reported
// as found=false and target is left untouched.
func (s *SnapshotStore) Load(lineID model.LineID, target any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, string(lineID)+".json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, target); err != nil {
		return false, fmt.Errorf("decode line snapshot: %w", err)
	}
	return true, nil
}
