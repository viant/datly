package jobs

import (
	"context"
	"errors"
	"fmt"
	dexec "github.com/viant/datly/exec"
	xasync "github.com/viant/xdatly/async"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
	"time"
)

type Action string

const (
	Submit  Action = "submit"
	Replay  Action = "replay"
	Inspect Action = "inspect"
)

// Access carries a detached public record and, for submit/replay, fresh canonical
// input. Authorization must revalidate current access and refresh any captured
// authorization claims on Input. Persisted principal fields alone are not proof.
type Access struct {
	Action Action
	Job    *xasync.Job
	Input  any
}
type Authorizer func(context.Context, Access) error

type Config struct {
	Store     *SQLStore
	Publisher Publisher
	Authorize Authorizer
	TTL       time.Duration
	ErrorTTL  time.Duration
	// Notify receives terminal state only after durable completion write-back.
	// Failure is returned without converting completed work back to ERROR.
	Notify func(context.Context, *xasync.Job) error
}
type Submission struct {
	Job   xasync.Job
	Input any
	// SourceState supplies original parameter-name-keyed raw values for codec
	// inputs or bodies whose authored presence cannot be recovered from a value.
	SourceState string
	Source      dexec.ProviderScope
	Policy      InputPolicy
}
type Scheduled struct {
	Job  *xasync.Job
	Plan *dexec.ReadPlan
}

// Dispatcher adapts jobs to the canonical runtime, never to a second SQL engine.
type Dispatcher interface {
	Capture(context.Context, *Record, Submission) (*Capture, error)
	Restore(context.Context, *Record) (Execution, error)
}
type Execution interface {
	Execute(context.Context) (*ExecutionResult, error)
}

// ExecutionResult retains canonical completion evidence without granting
// transaction handles or completion controls to the job transport.
type ExecutionResult struct {
	Value   any
	Outcome *xhandler.Outcome
	Metrics xresponse.Metrics
}

var ErrCompletionPending = errors.New("job completion requires managed transaction reconciliation")

type Service struct {
	config     Config
	dispatcher Dispatcher
}

func NewService(config Config, dispatcher Dispatcher) (*Service, error) {
	if config.Store == nil || config.Authorize == nil || dispatcher == nil {
		return nil, fmt.Errorf("async jobs require SQL store, explicit authorization and canonical dispatcher")
	}
	if config.TTL < 0 || config.ErrorTTL < 0 {
		return nil, fmt.Errorf("async expiry durations cannot be negative")
	}
	if config.TTL == 0 {
		config.TTL = time.Hour
	}
	if config.ErrorTTL == 0 {
		config.ErrorTTL = 10 * time.Second
	}
	return &Service{config: config, dispatcher: dispatcher}, nil
}
func (s *Service) Status(ctx context.Context, id string) (*xasync.Job, error) {
	record, err := s.inspect(ctx, inspection{id: id})
	if err != nil {
		return nil, err
	}
	return record.Public()
}
func (s *Service) Run(ctx context.Context, id string) (result any, err error) {
	record, err := s.config.Store.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if record.Status != xasync.StatusPending || !record.active(time.Now().UTC()) {
		return nil, ErrTransition
	}
	execution, err := s.dispatcher.Restore(ctx, record)
	if err != nil {
		return nil, err
	} // denied replay must not poison someone else's pending job
	if err := s.config.Store.claim(ctx, record, time.Now().UTC()); err != nil {
		return nil, err
	}
	var completion *xhandler.Outcome
	defer func() {
		if panicValue := recover(); panicValue != nil {
			err = dexec.NewPanicError("async execution", panicValue)
			result = nil
		}
		if completion != nil {
			if completion.Error != nil && !errors.Is(err, completion.Error) {
				err = errors.Join(err, completion.Error)
			}
			if completion.State() == xhandler.TransactionRolledBack && err == nil {
				err = fmt.Errorf("managed job transaction was rolled back")
			}
			if completion.State() == xhandler.TransactionCommitted && !completion.CommitConfirmed() && err == nil {
				err = fmt.Errorf("managed job transaction reported completion errors")
			}
			switch completion.State() {
			case xhandler.TransactionNone, xhandler.TransactionCommitted, xhandler.TransactionRolledBack:
			default:
				err = errors.Join(err, ErrCompletionPending)
				return
			}
		} else {
			err = errors.Join(err, ErrCompletionPending)
			return
		}
		record.finish(time.Now().UTC(), err, s.config.TTL, s.config.ErrorTTL)
		completionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if storeErr := s.config.Store.complete(completionCtx, record); storeErr != nil {
			err = errors.Join(err, ErrCompletionPending, storeErr)
			return
		}
		if s.config.Notify != nil {
			public, publicErr := record.Public()
			if publicErr != nil {
				err = errors.Join(err, publicErr)
				return
			}
			err = errors.Join(err, s.config.Notify(completionCtx, public))
		}
	}()
	// Replay has a new invocation context: no request, ambient input, transaction,
	// component stack or caller DI state leaks into the scheduled execution.
	executionCtx, cancel := context.WithCancel(context.Background())
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	defer cancel()
	if ctx.Err() != nil {
		cancel()
	}
	public, publicErr := record.Public()
	if publicErr != nil {
		return nil, publicErr
	}
	executionCtx = (Invocation{Job: public, Type: xasync.InvocationTypeEvent}).Context(executionCtx)
	executed, executionErr := execution.Execute(executionCtx)
	if executed != nil {
		completion = executed.Outcome
		result = executed.Value
		if metricErr := record.captureMetrics(executed.Metrics); metricErr != nil {
			executionErr = errors.Join(executionErr, metricErr)
		}
	}
	return result, executionErr
}
