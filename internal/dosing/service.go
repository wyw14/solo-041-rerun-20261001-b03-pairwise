package dosing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-97/internal/journal"
	"github.com/wyw14/cry-97/internal/model"
)

// commandNamespace seeds deterministic device command ids derived from a
// committed dosing change, so replaying the event log restores the exact same
// command instead of inventing a new one.
var commandNamespace = uuid.NewSHA1(uuid.NameSpaceOID, []byte("biotreat.dosing.command"))

// CommandFor builds the device command implied by a committed dosing change.
func CommandFor(change Change) model.DeviceCommand {
	command, _ := model.NewDeviceCommand(change.LineID, change.BatchID, "doser-"+change.Chemical,
		model.DeviceDoser, change.Generation, "set", change.RequestedAt)
	command.ID = uuid.NewSHA1(commandNamespace, []byte(change.ID.String()+":"+change.Chemical))
	command.Setpoint = change.Rate
	command.Metadata["change_id"] = change.ID.String()
	return command
}

type CommandPublisher interface {
	Publish(context.Context, model.DeviceCommand) error
}

type Service struct {
	journal   journal.Appender
	publisher CommandPublisher
	ledger    *Ledger
}

func NewService(appender journal.Appender, publisher CommandPublisher, ledger *Ledger) (*Service, error) {
	if appender == nil || publisher == nil || ledger == nil {
		return nil, errors.New("dosing service dependencies are required")
	}
	return &Service{journal: appender, publisher: publisher, ledger: ledger}, nil
}

func (s *Service) ChangeRate(ctx context.Context, change Change, now time.Time) (model.DeviceCommand, model.Event, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, model.Event{}, err
	}
	event, err := model.NewEvent(change.LineID, change.Generation, model.EventDosingChanged, change, now)
	if err != nil {
		return model.DeviceCommand{}, model.Event{}, err
	}
	persisted, err := s.journal.Append(ctx, event)
	if err != nil {
		return model.DeviceCommand{}, model.Event{}, fmt.Errorf("append dosing batch failed: %w", err)
	}
	command := CommandFor(change)
	if err := s.publisher.Publish(ctx, command); err != nil {
		return model.DeviceCommand{}, model.Event{}, fmt.Errorf("publish dosing command: %w", err)
	}
	s.ledger.Record(change)
	return command, persisted, nil
}

func (s *Service) Latest(batchID uuid.UUID) (Change, bool) {
	return s.ledger.Latest(batchID)
}

// Replay rebuilds dosing state from a committed dosing event. It never
// publishes a device command: commands are materialized separately from the
// event log during recovery.
func (s *Service) Replay(change Change) {
	s.ledger.Replay(change)
}

// History returns the committed dosing changes for a batch in request order.
func (s *Service) History(batchID uuid.UUID) []Change {
	return s.ledger.History(batchID)
}

// ResetLine drops restored dosing state for a line so recovery can rebuild it
// deterministically from the event log.
func (s *Service) ResetLine(lineID model.LineID) {
	s.ledger.ResetLine(lineID)
}
