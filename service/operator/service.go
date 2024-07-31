package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/datly/repository"
	rasync "github.com/viant/datly/repository/async"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/service"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/exec"
	"github.com/viant/xdatly/handler/response"
	"google.golang.org/api/googleapi"
	"net/http"
	"reflect"
)

type Service struct {
	fs afs.Service
}

// Operate processes data component with data session
func (s *Service) Operate(ctx context.Context, aSession *session.Session, aComponent *repository.Component) (interface{}, error) {
	if err := s.updateBackgroundJob(ctx, aComponent); err != nil {
		return nil, err
	}
	return s.operate(ctx, aComponent, aSession)
}

// HandleError processes output with error
func (s *Service) HandleError(ctx context.Context, aSession *session.Session, aComponent *repository.Component, err error) (interface{}, error) {

	ctx = context.WithValue(ctx, exec.ErrorKey, err)
	ctx = aComponent.View.Context(ctx)
	execCtx := exec.GetContext(ctx)
	if execCtx != nil {
		execCtx.StatusCode = http.StatusInternalServerError
		if rErr := errors.Unwrap(err); rErr != nil {
			switch actual := rErr.(type) {
			case *googleapi.Error:
				execCtx.StatusCode = actual.Code
			}
		}

	}
	output := aComponent.Output.Type.Type().NewState()
	var locatorOptions []locator.Option
	locatorOptions = append(locatorOptions,
		locator.WithView(aComponent.View),
		locator.WithTypes(aComponent.Types()...),
		locator.WithCustomOption(&response.Status{Status: "error", Message: err.Error(), Errors: []string{err.Error()}}),
		locator.WithParameterLookup(func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error) {
			return aSession.LookupValue(ctx, parameter, aSession.Indirect(true, locatorOptions...))
		}))
	var options = aSession.Indirect(true, locatorOptions...)
	err = aSession.SetState(ctx, aComponent.Output.Type.Parameters, output, options)
	return output.State(), err
}

func (s *Service) operate(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
	var err error
	ctx, err = s.EnsureContext(ctx, aSession, aComponent)
	if err != nil {
		return nil, err
	}
	if ctx, err = s.EnsureAsyncContext(ctx, aSession, aComponent); err != nil {
		return nil, err
	}
	switch aComponent.Service {
	case service.TypeReader:
		if ctx, err = s.EnsureInput(ctx, aComponent, aSession, true); err != nil {
			return nil, err
		}
		if err != nil {
			return s.HandleError(ctx, aSession, aComponent, err)
		}
		ret, err := s.runQuery(ctx, aComponent, aSession)
		if ret, err = s.finalize(ctx, ret, err); err != nil {
			aSession.ClearCache(aComponent.Output.Type.Parameters)
			return s.HandleError(ctx, aSession, aComponent, err)
		}
		return ret, err
	case service.TypeExecutor:
		if ctx, err = s.EnsureInput(ctx, aComponent, aSession, false); err != nil {
			return nil, err
		}
		ret, err := s.execute(ctx, aComponent, aSession)
		return s.finalize(ctx, ret, err)
	}
	return nil, response.NewError(500, fmt.Sprintf("unsupported Type %v", aComponent.Service))
}

func (s *Service) finalize(ctx context.Context, ret interface{}, err error) (interface{}, error) {
	if err != nil {
		return nil, err
	}
	if finalizer, ok := ret.(state.Finalizer); ok {
		err = finalizer.Finalize(ctx)
	}
	return ret, err
}

func (s *Service) EnsureContext(ctx context.Context, aSession *session.Session, aComponent *repository.Component) (context.Context, error) {
	ctx = codec.NewCriteriaBuilder(ctx, reader.New())
	ctx = context.WithValue(ctx, view.ContextKey, aComponent.View)
	ctx = aSession.Context(ctx, false)
	var info *exec.Context
	infoValue := ctx.Value(exec.ContextKey)
	if infoValue == nil {
		info = exec.NewContext()
		ctx = context.WithValue(ctx, exec.ContextKey, info)
	} else {
		info = infoValue.(*exec.Context)
	}

	if aComponent.Input.IgnoreEmptyQueryParameters {
		if value := ctx.Value(exec.ContextKey); value != nil {
			info.IgnoreEmptyQueryParameters = true
		}
	}
	return ctx, nil
}

func (s *Service) EnsureInput(ctx context.Context, aComponent *repository.Component, aSession *session.Session, populateView bool) (context.Context, error) {
	if inputType := aComponent.Input.Type; inputType.Type() != nil {
		var inputState *structology.State
		input := ctx.Value(xhandler.InputKey)
		if input != nil {
			inputState = inputType.Type().WithValue(input)
		} else {
			inputState = inputType.Type().NewState()
		}

		locatorOptions := aComponent.LocatorOptions(nil, nil, nil)
		options := aSession.ViewOptions(aComponent.View, session.WithLocatorOptions(locatorOptions...))
		options = options.Indirect(true)
		if populateView {
			if err := aSession.Populate(ctx); err != nil {
				return ctx, err
			}
		}
		err := aSession.SetState(ctx, inputType.Parameters, inputState, options)
		if err != nil {
			return nil, err
		}
		if input == nil {
			anInput := inputState.State()
			if reflect.TypeOf(anInput).Kind() == reflect.Struct {
				inputState.SyncPointer()
				anInput = inputState.StatePtr()
			}
			if initer, ok := anInput.(state.Initializer); ok {
				if err = initer.Init(ctx); err != nil {
					return nil, err
				}
			}
			ctx = context.WithValue(ctx, xhandler.InputKey, anInput)
		}
	}
	return ctx, nil
}

func (s *Service) EnsureAsyncContext(ctx context.Context, aSession *session.Session, aComponent *repository.Component) (context.Context, error) {
	infoValue := ctx.Value(exec.ContextKey)
	if infoValue == nil {
		return ctx, nil
	}
	info := infoValue.(*exec.Context)
	s.ensureContentSetting(ctx, aSession, aComponent)
	asyncModule := aComponent.Async
	if asyncModule == nil {
		return ctx, nil
	}
	return s.ensureAsyncContext(ctx, aSession, aComponent, asyncModule, info)
}

func (s *Service) ensureContentSetting(ctx context.Context, aSession *session.Session, aComponent *repository.Component) {
	settings := aSession.State().QuerySettings(aComponent.View)
	if settings != nil && settings.ContentFormat == "" {
		settings.ContentFormat = aComponent.Output.DataFormat
	}
	execContext := ctx.Value(exec.ContextKey).(*exec.Context)
	if value, has := execContext.Value(view.SyncFlag); has {
		settings.SyncFlag = value.(bool)
	}
	switch settings.ContentFormat { //fore sync response for the following content types
	case content.XLSFormat:
		execContext.SetValue(view.SyncFlag, true)
		_ = aSession.SetCacheValue(ctx, aComponent.View.Selector.GetSyncFlagParameter(), true)
		settings.SyncFlag = true
	}
}

func (s *Service) ensureAsyncContext(ctx context.Context, aSession *session.Session, aComponent *repository.Component, asyncModule *rasync.Config, info *exec.Context) (context.Context, error) {
	if job := ctx.Value(async.JobKey); job != nil {
		return ctx, nil
	}
	asyncModule.Lock()
	defer asyncModule.Unlock()

	aState := aComponent.Input.Type.Type().NewState()
	external := aComponent.Input.Type.Parameters.External()
	options := aSession.ViewOptions(aComponent.View)
	options = options.Indirect(true)
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
		settings := aSession.State().QuerySettings(aComponent.View)
		if settings != nil && !settings.SyncFlag {
			if err = s.publishEvent(ctx, asyncModule, job); err != nil {
				return nil, err
			}
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
