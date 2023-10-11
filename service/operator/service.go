package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/datly/repository"
	rasync "github.com/viant/datly/repository/async"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/operator/exec"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/async"
)

type Service struct {
	fs afs.Service
}

// Operate processes data component with data session
func (s *Service) Operate(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
	if err := s.updateJobStatusRunning(ctx, aComponent); err != nil {
		return nil, err
	}
	result, err := s.operate(ctx, aComponent, aSession)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) operate(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
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
	var info *exec.Info
	if ctx.Value(exec.InfoKey) == nil {
		ctx = context.WithValue(ctx, exec.InfoKey, &exec.Info{})
	}
	if infoValue := ctx.Value(exec.InfoKey); infoValue != nil {
		info = infoValue.(*exec.Info)
	}
	s.ensureContentSetting(ctx, aSession, aComponent)
	asyncModule := aComponent.Async
	if asyncModule == nil {
		return ctx, nil
	}
	return s.ensureAsyncContext(ctx, aComponent, aSession, asyncModule, info)
}

func (s *Service) ensureContentSetting(ctx context.Context, aSession *session.Session, aComponent *repository.Component) {
	settings := aSession.State().QuerySettings(aComponent.View)
	if settings != nil && settings.ContentFormat == "" {
		settings.ContentFormat = aComponent.Output.DataFormat
	}
	switch settings.ContentFormat { //fore sync response for the following content types
	case content.XLSFormat:
		_ = aSession.SetCacheValue(ctx, aComponent.View.Selector.GetContentFormatParameter(), settings.ContentFormat)
		settings.SyncFlag = true
	}
}

func (s *Service) ensureAsyncContext(ctx context.Context, aComponent *repository.Component, aSession *session.Session, asyncModule *rasync.Config, info *exec.Info) (context.Context, error) {
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

	matchKey, err := aState.String(asyncModule.JobMatchKey.Name)
	if err != nil {
		return nil, err
	}
	matchKey = aComponent.View.Name + "/" + matchKey
	job, err := aComponent.Async.JobByMatchKey(ctx, matchKey)
	if err != nil {
		return nil, err
	}
	if job == nil {
		if job, err = s.buildJob(ctx, aSession, aState, aComponent, matchKey, options); err != nil {
			return nil, err
		}
		destURL := asyncModule.DestinationURL(job)
		job.EventURL = destURL
		if err = asyncModule.CreateJob(ctx, job, &asyncModule.Notification); err != nil {
			return nil, err
		}
		if err = s.publishEvent(ctx, asyncModule, job); err != nil {
			return nil, err
		}
	}
	info.AppendJob(job)
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
