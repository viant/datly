package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/afs/url"
	xasync "github.com/viant/xdatly/async"
	"time"
)

// Event preserves the original .job document shape, including State and SQL.
// Public SDK JSON names are case-insensitively readable by original Datly.
type Event struct {
	xasync.Job
	State   string
	Metrics string
	SQL     json.RawMessage
}

func (r *Record) Event() (*Event, error) {
	public, err := r.Public()
	if err != nil {
		return nil, err
	}
	query := json.RawMessage(r.SQLQuery)
	if len(query) == 0 {
		query = json.RawMessage("null")
	}
	if !json.Valid(query) {
		return nil, fmt.Errorf("invalid persisted SQLQuery JSON")
	}
	return &Event{Job: *public, State: r.State, Metrics: r.Metrics, SQL: query}, nil
}

// Publisher owns dispatch-event publication, separate from terminal notification.
type Publisher interface {
	Prepare(*Record) error
	Publish(context.Context, *Record) error
}

type PublicationError struct {
	ID, EventURL string
	Err          error
}

func (e *PublicationError) Error() string {
	return fmt.Sprintf("publish job %s at %s: %v", e.ID, e.EventURL, e.Err)
}
func (e *PublicationError) Unwrap() error { return e.Err }

var ErrInProgress = errors.New("job execution is already in progress")
var ErrJobFailed = errors.New("job execution failed")

// HandleJob is the common entry point for local and cloud storage dispatch.
// Event contents identify a durable record; replay state and route authority are
// always loaded from DATLY_JOBS, never trusted from the uploaded document.
func (s *Service) HandleJob(ctx context.Context, event *Event) (any, error) {
	if event == nil || event.ID == "" {
		return nil, fmt.Errorf("storage event requires a job ID")
	}
	record, err := s.config.Store.Get(ctx, event.ID)
	if err != nil {
		return nil, err
	}
	if event.Method != record.Method || event.URI != record.URI || event.EventURL == "" || !url.Equals(event.EventURL, record.EventURL) {
		return nil, fmt.Errorf("storage event does not match durable job identity")
	}
	public, err := record.Public()
	if err != nil {
		return nil, err
	}
	if err := s.config.Authorize(ctx, Access{Action: Inspect, Job: public}); err != nil {
		return nil, err
	}
	if record.Status == xasync.StatusDone {
		return nil, nil
	}
	if record.Status == xasync.StatusError {
		return nil, ErrJobFailed
	}
	if record.Status == xasync.StatusRunning {
		return nil, ErrInProgress
	}
	result, err := s.Run(ctx, event.ID)
	if !errors.Is(err, ErrTransition) {
		return result, err
	}
	current, readErr := s.config.Store.Get(ctx, event.ID)
	if readErr != nil {
		return nil, errors.Join(err, readErr)
	}
	switch current.Status {
	case xasync.StatusDone:
		return nil, nil
	case xasync.StatusRunning:
		return nil, ErrInProgress
	case xasync.StatusError:
		return nil, ErrJobFailed
	}
	return result, err
}

// Republish retries transport delivery for an authorized pending durable job.
// It does not reset a running/terminal job or repeat application execution.
func (s *Service) Republish(ctx context.Context, id string) error {
	if s.config.Publisher == nil {
		return fmt.Errorf("job publisher is not configured")
	}
	record, err := s.config.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	if record.Status != xasync.StatusPending || !record.active(time.Now().UTC()) {
		return ErrTransition
	}
	if _, err := s.dispatcher.Restore(ctx, record); err != nil {
		return err
	}
	return s.config.Publisher.Publish(ctx, record)
}
