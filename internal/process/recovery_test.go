package process

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-97/internal/model"
)

func newTestPlant(t *testing.T, dir string) *Plant {
	t.Helper()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	clock := func() time.Time { return base }
	plant, err := NewPlant(dir, []model.LineID{"line-1", "line-2"}, clock)
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	return plant
}

func restartPlant(t *testing.T, dir string) *Plant {
	t.Helper()
	plant := newTestPlant(t, dir)
	for _, lineID := range []model.LineID{"line-1", "line-2"} {
		if _, err := plant.RecoverLine(context.Background(), lineID); err != nil {
			t.Fatalf("recover %s: %v", lineID, err)
		}
	}
	return plant
}

// The reported incident: line A opened a batch and moved it to equalizing;
// after a restart and recovery the event sequence advanced but the line still
// showed receiving with no active batch.
func TestRecoverRebuildsBatchStageAndResources(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	live := newTestPlant(t, dir)

	batch, err := live.StartBatch(ctx, "line-1", 500, "inlet")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if _, err := live.AdvanceBatch(ctx, "line-1", model.StageEqualizing); err != nil {
		t.Fatalf("advance batch: %v", err)
	}

	recovered := restartPlant(t, dir)

	state, err := recovered.Status("line-1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if state.Line.Stage != model.StageEqualizing {
		t.Fatalf("stage = %s, want equalizing", state.Line.Stage)
	}
	if state.Batch == nil || state.Batch.ID != batch.ID {
		t.Fatalf("active batch not recovered: %+v", state.Batch)
	}
	if state.Line.Generation != 1 {
		t.Fatalf("generation = %d, want 1", state.Line.Generation)
	}
	if state.Line.LastEventSeq != 2 {
		t.Fatalf("last event seq = %d, want 2", state.Line.LastEventSeq)
	}
	var assigned bool
	for _, basin := range state.Equalizers {
		if basin.ID == "EQ-line-1" && basin.LineID == "line-1" && basin.AssignedFlow == 500 {
			assigned = true
		}
	}
	if !assigned {
		t.Fatalf("equalizer assignment not recovered: %+v", state.Equalizers)
	}
	if state.Intake == nil || state.Intake.EqualizerID != "EQ-line-1" {
		t.Fatalf("intake admission not recovered: %+v", state.Intake)
	}
}

// Recovery restores command projections without dispatching, and a second
// recovery must not produce duplicate device commands.
func TestRecoverDoesNotReissueCommandsAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	live := newTestPlant(t, dir)

	batch, err := live.StartBatch(ctx, "line-1", 500, "inlet")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	command, err := live.ChangeDose(ctx, "line-1", batch.ID, "pac", 12.5)
	if err != nil {
		t.Fatalf("change dose: %v", err)
	}

	recovered := restartPlant(t, dir)
	first := recovered.devices.ForLine("line-1")
	if len(first) != 1 || first[0].ID != command.ID {
		t.Fatalf("command not restored deterministically: %+v", first)
	}
	if first[0].Setpoint != 12.5 || first[0].Action != "set" {
		t.Fatalf("restored command mismatch: %+v", first[0])
	}

	// Repeated recovery: reset + replay must converge on the same projection.
	if _, err := recovered.RecoverLine(ctx, "line-1"); err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	second := recovered.devices.ForLine("line-1")
	if len(second) != 1 || second[0].ID != command.ID {
		t.Fatalf("repeated recovery changed commands: %+v", second)
	}
	if dose, ok := recovered.dosing.Latest(batch.ID); !ok || dose.Rate != 12.5 {
		t.Fatalf("dosing ledger not recovered: %+v ok=%v", dose, ok)
	}
}

// Event sequence must keep growing after recovery.
func TestEventSequenceContinuesAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	live := newTestPlant(t, dir)

	batch, err := live.StartBatch(ctx, "line-1", 500, "inlet")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if _, err := live.AdvanceBatch(ctx, "line-1", model.StageEqualizing); err != nil {
		t.Fatalf("advance batch: %v", err)
	}
	if _, err := live.ChangeDose(ctx, "line-1", batch.ID, "pac", 12.5); err != nil {
		t.Fatalf("change dose: %v", err)
	}

	recovered := restartPlant(t, dir)
	if _, err := recovered.ChangeDose(ctx, "line-1", batch.ID, "pac", 15); err != nil {
		t.Fatalf("change dose after recovery: %v", err)
	}
	line := recovered.Lines()[0]
	for _, candidate := range recovered.Lines() {
		if candidate.ID == "line-1" {
			line = candidate
		}
	}
	if line.LastEventSeq != 4 {
		t.Fatalf("last event seq after new command = %d, want 4", line.LastEventSeq)
	}
	events, err := recovered.store.Events(ctx, "line-1")
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 4 || events[3].Sequence != 4 {
		t.Fatalf("journal sequence did not continue: %+v", events)
	}
}

// Emergency isolation bumps the generation, releases intake resources and
// marks active commands stopped; recovery must reproduce all of it.
func TestRecoverEmergencyIsolation(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	live := newTestPlant(t, dir)

	batch, err := live.StartBatch(ctx, "line-1", 500, "inlet")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if _, err := live.ChangeDose(ctx, "line-1", batch.ID, "pac", 12.5); err != nil {
		t.Fatalf("change dose: %v", err)
	}
	if _, err := live.EmergencyStop(ctx, "line-1", "toxic load"); err != nil {
		t.Fatalf("emergency stop: %v", err)
	}

	recovered := restartPlant(t, dir)
	status, err := recovered.Status("line-1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Line.Stage != model.StageIsolated || !status.Line.Emergency || status.Line.Generation != 2 {
		t.Fatalf("line not isolated correctly: %+v", status.Line)
	}
	if status.Batch == nil || status.Batch.Stage != model.StageIsolated {
		t.Fatalf("batch not marked isolated: %+v", status.Batch)
	}
	if status.Intake != nil {
		t.Fatalf("intake admission should be released: %+v", status.Intake)
	}
	for _, basin := range status.Equalizers {
		if basin.ID == "EQ-line-1" && basin.LineID == "line-1" {
			t.Fatalf("equalizer still assigned after isolation: %+v", basin)
		}
	}
	var command model.DeviceCommand
	for _, candidate := range status.Commands {
		if candidate.LineID == "line-1" {
			command = candidate
		}
	}
	if command.Action != "stop" || command.Generation != 2 || command.Setpoint != 0 {
		t.Fatalf("command not stopped by isolation: %+v", command)
	}

	// An isolated line keeps its generation across another repeated recovery.
	if _, err := recovered.RecoverLine(ctx, "line-1"); err != nil {
		t.Fatalf("repeat recovery: %v", err)
	}
	again, _ := recovered.Status("line-1")
	if again.Line.Generation != 2 || again.Line.Stage != model.StageIsolated {
		t.Fatalf("generation/stage drifted after repeat recovery: %+v", again.Line)
	}
}

// Repeated recoveries serialize and converge: after a snapshot is written,
// events committed later are folded on the next recovery without resetting
// earlier state.
func TestRecoverFoldsEventsAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	live := newTestPlant(t, dir)

	batch, err := live.StartBatch(ctx, "line-1", 500, "inlet")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if _, err := live.ChangeDose(ctx, "line-1", batch.ID, "pac", 12.5); err != nil {
		t.Fatalf("change dose: %v", err)
	}

	recovered := restartPlant(t, dir) // writes a snapshot at seq 2
	if _, err := recovered.AdvanceBatch(ctx, "line-1", model.StageEqualizing); err != nil {
		t.Fatalf("advance after first recovery: %v", err)
	}
	// Another recovery (e.g. a second restart) must fold only seq 3 and keep
	// the dosing projection from the snapshot intact.
	if _, err := recovered.RecoverLine(ctx, "line-1"); err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	status, err := recovered.Status("line-1")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if status.Line.Stage != model.StageEqualizing || status.Line.LastEventSeq != 3 {
		t.Fatalf("post-snapshot events not folded: %+v", status.Line)
	}
	if status.Batch == nil || status.Batch.ID != batch.ID {
		t.Fatalf("batch lost across snapshot boundary: %+v", status.Batch)
	}
	if status.Dose == nil || status.Dose.Rate != 12.5 {
		t.Fatalf("dosing state lost across snapshot boundary: %+v", status.Dose)
	}
	if commands := recovered.devices.ForLine("line-1"); len(commands) != 1 {
		t.Fatalf("command projection drifted across snapshot boundary: %+v", commands)
	}
}

// Live mutations are blocked from interleaving with a running recovery.
func TestMutationBlockedDuringRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	plant := newTestPlant(t, dir)

	release := make(chan struct{})
	entered := make(chan struct{})
	go func() {
		plant.recoveryMu.Lock() // simulate a recovery holding the write lock
		close(entered)
		<-release
		plant.recoveryMu.Unlock()
	}()

	<-entered
	done := make(chan error, 1)
	go func() {
		_, err := plant.StartBatch(ctx, "line-1", 500, "inlet")
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("mutation did not wait for recovery: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("mutation after recovery: %v", err)
	}
}

func TestUnknownLineRecoveryRejected(t *testing.T) {
	plant := newTestPlant(t, t.TempDir())
	if _, err := plant.RecoverLine(context.Background(), model.LineID("line-9")); err == nil {
		t.Fatal("expected error for unconfigured line")
	}
}
