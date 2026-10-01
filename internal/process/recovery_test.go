package process

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/model"
)

func testPlant(t *testing.T, dir string) *Plant {
	t.Helper()
	clock := func() time.Time { return time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC) }
	plant, err := NewPlant(dir, []model.LineID{"line-1"}, clock)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func equalizerForLine(plant *Plant, lineID model.LineID) (string, bool) {
	for _, basin := range plant.equalizers.Snapshot() {
		if basin.LineID == lineID {
			return basin.ID, true
		}
	}
	return "", false
}

// A batch that advanced to equalizing must be rebuilt exactly as committed
// after a process restart, even though no snapshot was saved along the way.
func TestRecoverRebuildsBatchAndStage(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	plant := testPlant(t, dir)

	batch, err := plant.StartBatch(ctx, "line-1", 1000, "municipal")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if _, err := plant.AdvanceBatch(ctx, "line-1", model.StageEqualizing); err != nil {
		t.Fatalf("advance batch: %v", err)
	}

	// Simulate a restart: a brand new plant over the same journal directory.
	restarted := testPlant(t, dir)
	last, err := restarted.RecoverLine(ctx, "line-1")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if last != 2 {
		t.Fatalf("last event seq = %d, want 2", last)
	}

	state := restarted.lines["line-1"]
	if state.Stage != model.StageEqualizing {
		t.Fatalf("stage = %q, want equalizing", state.Stage)
	}
	if state.Generation != 1 {
		t.Fatalf("generation = %d, want 1", state.Generation)
	}
	if state.BatchID != batch.ID.String() {
		t.Fatalf("batch id = %q, want %q", state.BatchID, batch.ID)
	}
	if state.Emergency {
		t.Fatal("line should not be emergency")
	}

	active, ok := restarted.ActiveBatch("line-1")
	if !ok {
		t.Fatal("active batch missing after recovery")
	}
	if active.ID != batch.ID || active.Stage != model.StageEqualizing {
		t.Fatalf("recovered batch = %+v, want id %s at equalizing", active, batch.ID)
	}

	if id, ok := equalizerForLine(restarted, "line-1"); !ok {
		t.Fatal("equalizer occupancy not restored")
	} else if id != "EQ-line-1" {
		t.Fatalf("equalizer = %q, want EQ-line-1", id)
	}
	if admission, ok := restarted.intake.Current("line-1"); !ok || admission.Source != "municipal" {
		t.Fatalf("intake admission not restored: %+v ok=%v", admission, ok)
	}
}

// Recovery is a deterministic projection: running it repeatedly on the same
// process must converge on identical state.
func TestRecoverIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	plant := testPlant(t, dir)

	if _, err := plant.StartBatch(ctx, "line-1", 1000, "municipal"); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if _, err := plant.AdvanceBatch(ctx, "line-1", model.StageEqualizing); err != nil {
		t.Fatalf("advance batch: %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, err := plant.RecoverLine(ctx, "line-1"); err != nil {
			t.Fatalf("recover #%d: %v", i+1, err)
		}
		state := plant.lines["line-1"]
		if state.Stage != model.StageEqualizing || state.Generation != 1 || state.LastEventSeq != 2 {
			t.Fatalf("recover #%d state = %+v", i+1, state)
		}
		if _, ok := plant.ActiveBatch("line-1"); !ok {
			t.Fatalf("recover #%d lost active batch", i+1)
		}
		if id, ok := equalizerForLine(plant, "line-1"); !ok || id != "EQ-line-1" {
			t.Fatalf("recover #%d equalizer wrong: %q ok=%v", i+1, id, ok)
		}
		if plant.devices.Dispatched() != 0 {
			t.Fatalf("replay dispatched commands on recover #%d", i+1)
		}
	}
}

// device.commanded events rebuild last-known device state on replay but must
// never be re-dispatched, including across repeated recoveries.
func TestRecoverDoesNotRedispatchCommands(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	plant := testPlant(t, dir)

	batch, err := plant.StartBatch(ctx, "line-1", 1000, "municipal")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	now := plant.now()
	command, err := model.NewDeviceCommand("line-1", batch.ID, "doser-pac", model.DeviceDoser, 1, "set", now)
	if err != nil {
		t.Fatalf("new command: %v", err)
	}
	command.Setpoint = 42
	event, err := model.NewEvent("line-1", 1, model.EventDeviceCommanded, command, now)
	if err != nil {
		t.Fatalf("new event: %v", err)
	}
	if _, err := plant.store.Append(ctx, event); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Restart and recover twice: the command is visible as last-known state,
	// but nothing is dispatched to a device during replay.
	restarted := testPlant(t, dir)
	for i := 0; i < 2; i++ {
		if _, err := restarted.RecoverLine(ctx, "line-1"); err != nil {
			t.Fatalf("recover #%d: %v", i+1, err)
		}
		if got := restarted.devices.Dispatched(); got != 0 {
			t.Fatalf("recover #%d dispatched %d commands", i+1, got)
		}
		commands := restarted.devices.All()
		if len(commands) != 1 || commands[0].DeviceID != "doser-pac" {
			t.Fatalf("recover #%d restored commands = %+v", i+1, commands)
		}
	}
}

// Emergency isolation bumps the generation exactly once and is stable across
// recovery; old-generation events afterwards cannot resurrect state.
func TestRecoverEmergencyGeneration(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	plant := testPlant(t, dir)

	if _, err := plant.StartBatch(ctx, "line-1", 1000, "municipal"); err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if _, err := plant.AdvanceBatch(ctx, "line-1", model.StageEqualizing); err != nil {
		t.Fatalf("advance batch: %v", err)
	}
	if _, err := plant.EmergencyStop(ctx, "line-1", "toxic load"); err != nil {
		t.Fatalf("emergency: %v", err)
	}

	restarted := testPlant(t, dir)
	for i := 0; i < 2; i++ {
		last, err := restarted.RecoverLine(ctx, "line-1")
		if err != nil {
			t.Fatalf("recover #%d: %v", i+1, err)
		}
		state := restarted.lines["line-1"]
		if !state.Emergency || state.Stage != model.StageIsolated {
			t.Fatalf("recover #%d state = %+v, want isolated emergency", i+1, state)
		}
		if state.Generation != 2 {
			t.Fatalf("recover #%d generation = %d, want 2", i+1, state.Generation)
		}
		if last != 3 {
			t.Fatalf("recover #%d last seq = %d, want 3", i+1, last)
		}
		if _, occupied := equalizerForLine(restarted, "line-1"); occupied {
			t.Fatalf("recover #%d equalizer still assigned after emergency", i+1)
		}
	}
}

// A stale device command from an older generation is ignored, and commands
// for other lines are left untouched by line-scoped recovery.
func TestRestoreIgnoresStaleAndForeignCommands(t *testing.T) {
	bus := NewDeviceBus()
	current, _ := model.NewDeviceCommand("line-1", uuid.New(), "doser-a", model.DeviceDoser, 2, "set", time.Now())
	bus.commands["doser-a"] = current
	bus.applied[current.ID] = struct{}{}

	stale, _ := model.NewDeviceCommand("line-1", uuid.New(), "doser-a", model.DeviceDoser, 1, "set", time.Now())
	bus.Restore(stale)
	if got := bus.commands["doser-a"]; got.ID != current.ID {
		t.Fatalf("stale command overwrote current: %+v", got)
	}

	foreign, _ := model.NewDeviceCommand("line-2", uuid.New(), "doser-b", model.DeviceDoser, 1, "set", time.Now())
	bus.commands["doser-b"] = foreign
	bus.applied[foreign.ID] = struct{}{}
	bus.ClearLine("line-1")
	if _, ok := bus.commands["doser-a"]; ok {
		t.Fatal("line command not cleared")
	}
	if _, ok := bus.commands["doser-b"]; !ok {
		t.Fatal("foreign command incorrectly cleared")
	}
}
