package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	dexec "github.com/viant/datly/exec"
	xasync "github.com/viant/xdatly/async"
	"time"
)

func (s *Service) Schedule(ctx context.Context, request Submission) (*Scheduled, error) {
	if request.Policy != nil {
		return nil, fmt.Errorf("scheduling controls require Exchange")
	}
	record, captured, err := s.prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	return s.persist(ctx, record, captured.Plan, true)
}
func (s *Service) prepare(ctx context.Context, request Submission) (*Record, *Capture, error) {
	// JSON detaches SDK pointer-valued metadata from the submitting caller.
	record := &Record{Job: request.Job}
	public, err := record.Public()
	if err != nil {
		return nil, nil, err
	}
	record.Job = *public
	if record.ID == "" {
		record.ID = uuid.NewString()
	}
	record.Status = xasync.StatusPending
	record.CreationTime = time.Now().UTC()
	record.StartTime, record.EndTime, record.Error = nil, nil, nil
	record.WaitTimeInMcs, record.RunTimeInMcs, record.Deactivated = 0, 0, false
	if record.ExpiryTime != nil && !record.ExpiryTime.After(record.CreationTime) {
		return nil, nil, fmt.Errorf("new job is already expired")
	}
	captured, err := s.dispatcher.Capture(ctx, record, request)
	if err != nil {
		return nil, nil, err
	}
	return record, captured, nil
}
func (s *Service) persist(ctx context.Context, record *Record, plan *dexec.ReadPlan, publish bool) (*Scheduled, error) {
	if plan != nil {
		query, err := json.Marshal([]struct {
			Query string
			Args  []any
		}{{Query: plan.SQL, Args: plan.Args}})
		if err != nil {
			return nil, err
		}
		record.SQLQuery = string(query)
	}
	if s.config.Publisher != nil {
		if err := s.config.Publisher.Prepare(record); err != nil {
			return nil, err
		}
	}
	if err := s.config.Store.Create(ctx, record); err != nil {
		return nil, err
	}
	if publish && s.config.Publisher != nil {
		if err := s.config.Publisher.Publish(ctx, record); err != nil {
			return nil, &PublicationError{ID: record.ID, EventURL: record.EventURL, Err: err}
		}
	}
	public, err := record.Public()
	return &Scheduled{Job: public, Plan: plan}, err
}
