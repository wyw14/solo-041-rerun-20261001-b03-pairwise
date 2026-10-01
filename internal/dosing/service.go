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

func (s *Service) ChangeRate(ctx context.Context, change Change, now time.Time) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	event, err := model.NewEvent(change.LineID, change.Generation, model.EventDosingChanged, change, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if _, err := s.journal.Append(ctx, event); err != nil {
		return model.DeviceCommand{}, fmt.Errorf("append dosing batch failed: %w", err)
	}
	command, err := model.NewDeviceCommand(change.LineID, change.BatchID, "doser-"+change.Chemical, model.DeviceDoser, change.Generation, "set", now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	command.Setpoint = change.Rate
	command.Metadata["change_id"] = change.ID.String()
	if err := s.publisher.Publish(ctx, command); err != nil {
		return model.DeviceCommand{}, fmt.Errorf("publish dosing command: %w", err)
	}
	s.ledger.Record(change)
	return command, nil
}

func (s *Service) Latest(batchID uuid.UUID) (Change, bool) {
	return s.ledger.Latest(batchID)
}

// Restore re-applies a dosing change captured by a committed event during
// replay. It rebuilds the ledger only and never re-publishes a device
// command, so replay is side-effect free.
func (s *Service) Restore(change Change) {
	s.ledger.Restore(change)
}

// ClearLine drops restored dosing changes for a line before re-projection.
func (s *Service) ClearLine(lineID model.LineID) {
	s.ledger.ClearLine(lineID)
}
