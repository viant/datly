package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	dexec "github.com/viant/datly/exec"
	xasync "github.com/viant/xdatly/async"
)

// InputPolicy selects scheduling controls from freshly bound canonical input.
// It is trusted registration metadata, never supplied by a transport client.
type InputPolicy interface {
	Select(context.Context, any) (Controls, error)
	Owns(*xasync.Job) bool
}
type Controls struct {
	MatchKey string
	JobID    string
	Sync     bool
	Result   bool
}
type Capture struct {
	Plan     *dexec.ReadPlan
	Input    any
	Controls Controls
}

// Exchange is the result of original schedule/sync/inspect orchestration.
// Value is present only when this invocation actually executed the job.
type Exchange struct {
	Job    *xasync.Job
	Plan   *dexec.ReadPlan
	Value  any
	Reused bool
}

// Exchange prepares external input once, then schedules, runs synchronously or
// inspects durable state. All execution still goes through Run's canonical claim,
// replay and completion path. No result replay or second transaction owner exists.
func (s *Service) Exchange(ctx context.Context, request Submission) (result *Exchange, failure error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			failure = fmt.Errorf("async job request panicked")
		}
	}()

	record, captured, err := s.prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	if captured.Controls.JobID != "" {
		stored, err := s.inspect(ctx, inspection{id: captured.Controls.JobID, input: captured.Input, policy: request.Policy})
		if err != nil {
			return nil, err
		}
		job, err := stored.Public()
		if err != nil {
			return nil, err
		}
		result = &Exchange{Job: job, Reused: true}
		if captured.Controls.Result {
			result.Value, err = s.readResult(ctx, ResultRequest{Job: stored, Source: request.Source, Refresh: captured.Controls.Sync})
		}
		return result, err
	}
	stored, reused, err := s.accept(ctx, record, captured)
	if err != nil {
		return nil, err
	}
	job, err := stored.Public()
	if err != nil {
		return nil, err
	}
	result = &Exchange{Job: job, Plan: captured.Plan, Reused: reused}
	if reused && job.Status == xasync.StatusDone {
		if job.JobType == "Reader" {
			result.Value, err = s.readResult(ctx, ResultRequest{Job: stored, Source: request.Source, SourceState: record.State, Refresh: captured.Controls.Sync})
			return result, err
		}
		if captured.Controls.Sync {
			return result, ErrResultUnavailable
		}
	}
	if !captured.Controls.Sync || job.Status == xasync.StatusError {
		return result, nil
	}
	if job.Status == xasync.StatusRunning {
		return result, ErrInProgress
	}
	result.Value, err = s.Run(ctx, job.ID)
	completionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	current, readErr := s.config.Store.Get(completionCtx, job.ID)
	if readErr == nil {
		if errors.Is(err, ErrTransition) {
			switch current.Status {
			case xasync.StatusRunning:
				err = ErrInProgress
			case xasync.StatusDone:
				err = nil
			case xasync.StatusError:
				err = ErrJobFailed
			}
		}
		result.Job, readErr = current.Public()
	}
	return result, errors.Join(err, readErr)
}

// ErrResultUnavailable distinguishes durable completion from retained result data.
var ErrResultUnavailable = errors.New("completed job result is not retained; use a new match key for synchronous execution")

func (s *Service) accept(ctx context.Context, record *Record, captured *Capture) (*Record, bool, error) {
	// Lookup/create serialization belongs to the actual shared store. Deferred
	// unlock also covers a policy or provider panic; Exchange reports it safely.
	s.config.Store.admission.Lock()
	defer s.config.Store.admission.Unlock()
	if record.MatchKey != "" {
		existing, err := s.config.Store.match(ctx, record, s.config.TTL, s.config.ErrorTTL)
		if err != nil {
			return nil, false, err
		}
		if existing != nil {
			job, err := s.inspect(ctx, inspection{record: existing, input: captured.Input, owner: record})
			return job, true, err
		}
	}
	_, err := s.persist(ctx, record, captured.Plan, !captured.Controls.Sync)
	if err != nil {
		return nil, false, err
	}
	return record, false, nil
}

type inspection struct {
	record *Record
	id     string
	input  any
	owner  *Record
	policy InputPolicy
}

func (s *Service) inspect(ctx context.Context, request inspection) (*Record, error) {
	id, input, owner, policy := request.id, request.input, request.owner, request.policy
	record := request.record
	var err error
	if record == nil {
		record, err = s.config.Store.Get(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	// A client may name a job, never another executable target. Restrict even
	// authorized inspection to the exact requesting registered route instance.
	if policy != nil && !policy.Owns(&record.Job) {
		return nil, ErrNotFound
	}
	if owner != nil && (record.Method != owner.Method || record.URI != owner.URI) {
		return nil, ErrNotFound
	}
	public, err := record.Public()
	if err != nil {
		return nil, err
	}
	if err = s.config.Authorize(ctx, Access{Action: Inspect, Job: public, Input: input}); err != nil {
		return nil, err
	}
	return record, nil
}
