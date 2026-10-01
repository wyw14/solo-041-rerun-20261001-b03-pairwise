package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
	"time"

	"github.com/wyw14/cry-97/internal/model"
	"github.com/wyw14/cry-97/internal/process"
)

func newTestServer(t *testing.T, dir string) (*Server, *process.Plant) {
	t.Helper()
	base := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	plant, err := process.NewPlant(dir, []model.LineID{"line-1"}, func() time.Time { return base })
	if err != nil {
		t.Fatalf("new plant: %v", err)
	}
	web := fstest.MapFS{"process.html": &fstest.MapFile{Data: []byte("ok")}}
	server, err := NewServer(plant, web)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	return server, plant
}

func TestRecoverEndpointRestoresCommittedState(t *testing.T) {
	dir := t.TempDir()
	_, plant := newTestServer(t, dir)
	ctx := context.Background()
	batch, err := plant.StartBatch(ctx, "line-1", 400, "inlet")
	if err != nil {
		t.Fatalf("start batch: %v", err)
	}
	if _, err := plant.AdvanceBatch(ctx, "line-1", model.StageEqualizing); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if _, err := plant.ChangeDose(ctx, "line-1", batch.ID, "pac", 9); err != nil {
		t.Fatalf("dose: %v", err)
	}

	// Simulate a restart: a fresh process (and HTTP server) over the same
	// data directory; state only exists in the journal/snapshot on disk.
	restarted, _ := newTestServer(t, dir)

	req := httptest.NewRequest(http.MethodPost, "/api/process-lines/line-1/recover", nil)
	rec := httptest.NewRecorder()
	restarted.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("recover status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		LineID       string `json:"line_id"`
		LastEventSeq uint64 `json:"last_event_seq"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.LineID != "line-1" || body.LastEventSeq != 3 {
		t.Fatalf("unexpected recover response: %+v", body)
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/api/process-lines/line-1", nil)
	statusRec := httptest.NewRecorder()
	restarted.Handler().ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status = %d", statusRec.Code)
	}
	var status struct {
		Line struct {
			Stage       string `json:"stage"`
			LastEventSq uint64 `json:"last_event_seq"`
		} `json:"line"`
		Batch *struct {
			ID string `json:"id"`
		} `json:"batch"`
		Commands []struct {
			Action   string  `json:"action"`
			Setpoint float64 `json:"setpoint"`
		} `json:"commands"`
	}
	if err := json.Unmarshal(statusRec.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if status.Line.Stage != "equalizing" || status.Line.LastEventSq != 3 {
		t.Fatalf("line not recovered: %+v", status.Line)
	}
	if status.Batch == nil || status.Batch.ID != batch.ID.String() {
		t.Fatalf("batch missing after recovery: %+v", status.Batch)
	}
	if len(status.Commands) != 1 || status.Commands[0].Setpoint != 9 {
		t.Fatalf("command projection wrong: %+v", status.Commands)
	}
}

func TestRecoverEndpointUnknownLine(t *testing.T) {
	server, _ := newTestServer(t, t.TempDir())
	req := httptest.NewRequest(http.MethodPost, "/api/process-lines/nope/recover", nil)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
}
