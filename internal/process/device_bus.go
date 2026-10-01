package process

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/wyw14/cry-97/internal/model"
)

type DeviceBus struct {
	mu       sync.RWMutex
	commands map[string]model.DeviceCommand
}

func NewDeviceBus() *DeviceBus {
	return &DeviceBus{commands: make(map[string]model.DeviceCommand)}
}

func (b *DeviceBus) Publish(ctx context.Context, command model.DeviceCommand) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if command.DeviceID == "" || command.Generation == 0 {
		return errors.New("device command is incomplete")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, exists := b.commands[command.DeviceID]
	if exists && command.Generation < current.Generation {
		return errors.New("device command generation is stale")
	}
	b.commands[command.DeviceID] = command
	return nil
}

// Restore rebuilds the command bus from a committed event. It performs no
// physical dispatch: replaying an event log must never re-issue commands.
// Replaying the same event is a no-op and older generations never overwrite
// newer ones, so repeated recovery is idempotent.
func (b *DeviceBus) Restore(command model.DeviceCommand) error {
	if command.DeviceID == "" || command.Generation == 0 {
		return errors.New("device command is incomplete")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	current, exists := b.commands[command.DeviceID]
	if exists && current.ID == command.ID {
		return nil
	}
	if exists && command.Generation < current.Generation {
		return nil
	}
	b.commands[command.DeviceID] = command
	return nil
}

// ResetLine clears the restored command state for a line so recovery can
// rebuild it deterministically from the event log.
func (b *DeviceBus) ResetLine(lineID model.LineID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, command := range b.commands {
		if command.LineID == lineID {
			delete(b.commands, id)
		}
	}
}

// ForLine returns a stable copy of the restored commands for one line.
func (b *DeviceBus) ForLine(lineID model.LineID) []model.DeviceCommand {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]model.DeviceCommand, 0)
	for _, command := range b.commands {
		if command.LineID == lineID {
			result = append(result, command)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].DeviceID == result[j].DeviceID {
			return result[i].IssuedAt.Before(result[j].IssuedAt)
		}
		return result[i].DeviceID < result[j].DeviceID
	})
	return result
}

func (b *DeviceBus) All() []model.DeviceCommand {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]model.DeviceCommand, 0, len(b.commands))
	for _, command := range b.commands {
		result = append(result, command)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].IssuedAt.Before(result[j].IssuedAt) })
	return result
}

func (b *DeviceBus) StopLine(lineID model.LineID, generation uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, command := range b.commands {
		if command.LineID != lineID {
			continue
		}
		command.Generation = generation
		command.Action = "stop"
		command.Setpoint = 0
		b.commands[id] = command
	}
}
