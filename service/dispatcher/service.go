package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/datly/repository"
	rasync "github.com/viant/datly/repository/async"
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/async"
)

type Service struct {
	fs afs.Service
}

func (s *Service) Dispatch(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
	if err := s.updateJobStatusRunning(ctx, aComponent); err != nil {
		return nil, err
	}
	result, err := s.dispatch(ctx, aComponent, aSession)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) dispatch(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
	var err error
	ctx, err = s.EnsureContext(ctx, aComponent, aSession)
	if err != nil {
		return nil, err
	}
	switch aComponent.Service {
	case service.TypeReader:
		return s.runQuery(ctx, aComponent, aSession)
	case service.TypeExecutor:
		return s.execute(ctx, aComponent, aSession)
	}
	return nil, httputils.NewHttpMessageError(500, fmt.Errorf("unsupported Type %v", aComponent.Service))
}

func (s *Service) EnsureContext(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (context.Context, error) {
	asyncModule := aComponent.Async
	if asyncModule == nil {
		return ctx, nil
	}
	if job := ctx.Value(async.JobKey); job != nil {
		return ctx, nil
	}
	asyncModule.Lock()
	defer asyncModule.Unlock()

	aState := aComponent.Input.Type.Type().NewState()
	external := aComponent.Input.Type.Parameters.External()
	options := aSession.Indirect(true, locator.WithState(aState))
	if err := aSession.SetState(ctx, external, aState, options); err != nil {
		return nil, err
	}

	jobRef, err := aState.String(asyncModule.JobRef.Name)
	job, err := aComponent.Async.JobByRef(ctx, jobRef)
	if err != nil {
		return nil, err
	}
	if job == nil {
		if job, err = s.buildJob(ctx, aSession, aComponent, jobRef, options); err != nil {
			return nil, err
		}
		destURL := asyncModule.DestinationURL(job)
		job.EventURL = destURL
		if err = asyncModule.CreateJob(ctx, job, &asyncModule.Notification); err != nil {
			return nil, err
		}
		if err := s.publishEvent(ctx, asyncModule, job); err != nil {
			return nil, err
		}
	}
	return context.WithValue(ctx, async.JobKey, job), nil
}

func (s *Service) publishEvent(ctx context.Context, asyncModule *rasync.Config, job *async.Job) error {
	switch asyncModule.Method {
	case async.NotificationMethodStorage:
		payload, err := json.Marshal(job)
		if err != nil {
			return err
		}
		if err = s.fs.Upload(ctx, job.EventURL, file.DefaultFileOsMode, bytes.NewReader(payload)); err != nil {
			return err
		}
	//case async.NotificationMethodMessageBus:
	default:
		return fmt.Errorf("unsupported event destination: %v", asyncModule.Method)
	}
	return nil
}

func New() *Service {
	return &Service{fs: afs.New()}
}
