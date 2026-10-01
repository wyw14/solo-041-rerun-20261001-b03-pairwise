package process

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

type DeviceBus struct {
	mu         sync.RWMutex
	commands   map[string]model.DeviceCommand
	applied    map[uuid.UUID]struct{}
	dispatched int
}

func NewDeviceBus() *DeviceBus {
	return &DeviceBus{commands: make(map[string]model.DeviceCommand), applied: make(map[uuid.UUID]struct{})}
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
	if err := b.apply(command); err != nil {
		return err
	}
	// Counts genuine dispatches only. Replay goes through Restore and must
	// never move this counter, even on a repeated recovery.
	b.dispatched++
	return nil
}

// Dispatched reports how many live device dispatches have occurred; it is
// unchanged by replay.
func (b *DeviceBus) Dispatched() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dispatched
}

// Restore re-applies a device command captured by a committed event during
// replay. It is idempotent by command ID: a command already delivered (for
// example by a repeated recovery) is never re-dispatched. Stale commands
// from a superseded generation are ignored rather than aborting replay.
func (b *DeviceBus) Restore(command model.DeviceCommand) {
	if command.DeviceID == "" || command.Generation == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, seen := b.applied[command.ID]; seen {
		return
	}
	if current, exists := b.commands[command.DeviceID]; exists && command.Generation < current.Generation {
		return
	}
	_ = b.apply(command)
}

// apply records a command. Caller must hold b.mu.
func (b *DeviceBus) apply(command model.DeviceCommand) error {
	if _, seen := b.applied[command.ID]; seen {
		return nil
	}
	current, exists := b.commands[command.DeviceID]
	if exists && command.Generation < current.Generation {
		return errors.New("device command generation is stale")
	}
	b.commands[command.DeviceID] = command
	b.applied[command.ID] = struct{}{}
	return nil
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

// ClearLine removes all derived command state for a line before
// re-projection. Restore never dispatches, so rebuilding the map on a
// repeated recovery cannot re-send a command to a device.
func (b *DeviceBus) ClearLine(lineID model.LineID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for deviceID, command := range b.commands {
		if command.LineID != lineID {
			continue
		}
		delete(b.applied, command.ID)
		delete(b.commands, deviceID)
	}
}
