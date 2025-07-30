package operator

import (
	"context"
	"fmt"
	"net/http"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/executor/handler"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/gmetric/counter"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/response"
	"reflect"
	"runtime/debug"
	"time"
)

// HandlerSession returns a handler session
func (s *Service) HandlerSession(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (xhandler.Session, error) {
	anExecutor := handler.NewExecutor(aComponent.View, aSession)
	return anExecutor.NewHandlerSession(ctx, handler.WithTypes(aComponent.Types()...),
		handler.WithLogger(aSession.Logger()),
		handler.WithAuth(aSession.Auth()))
}

func (s *Service) execute(ctx context.Context, aComponent *repository.Component, aSession *session.Session, onDone counter.OnDone) (interface{}, error) {
	anExecutor := handler.NewExecutor(aComponent.View, aSession)
	if aComponent.Handler != nil {
		result, err := s.executeHandler(ctx, aComponent, aSession, anExecutor)
		onDone(time.Now(), err)
		return result, err
	}

	executorSession, err := anExecutor.ExpandAndExecute(ctx)
	if err != nil {
		return nil, err
	}
	var responseValue interface{}

	if len(aComponent.Output.Type.Parameters) == 0 {
		return responseValue, nil
	}
	if stateType := aComponent.Output.Type.Type(); stateType != nil && stateType.IsDefined() {
		responseState := aComponent.Output.Type.Type().NewState()
		statelet := executorSession.Session.State().Lookup(executorSession.View)

		status := contract.StatusSuccess(executorSession.TemplateState)
		if err := aSession.SetState(ctx, aComponent.Output.Type.Parameters, responseState, aSession.Indirect(true,
			locator.WithCustom(&status),
			locator.WithLogger(aSession.Logger()),
			locator.WithState(statelet.Template))); err != nil {
			return nil, fmt.Errorf("failed to set response %w", err)
		}
		responseValue = responseState.State()
		if parameter := aComponent.Output.Type.AnonymousParameters(); parameter != nil {
			if responseValue, err = responseState.Value(parameter.Name); err != nil {
				return nil, err
			}
		}
	}
	return responseValue, nil
}

func (s *Service) executeHandler(ctx context.Context, aComponent *repository.Component, aSession *session.Session, anExecutor *handler.Executor) (output interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			if output == nil {
				output = s.ensureOutputInstance(aComponent, output)
			}
			panicMsg := fmt.Sprintf("Panic occurred: %v, Stack trace: %v", r, string(debug.Stack()))
			aSession.Logger().Errorc(ctx, panicMsg)
			if setter, ok := output.(response.StatusSetter); ok {
				setter.SetError(response.NewError(http.StatusInternalServerError, "Internal server error"))
			}
			err = response.NewError(http.StatusInternalServerError, "Internal server error")
		}
	}()

	aSession.SetView(aComponent.View)
	sessionHandler, err := anExecutor.NewHandlerSession(ctx,
		handler.WithLogger(aSession.Logger()),
		handler.WithTypes(aComponent.Types()...), handler.WithAuth(aSession.Auth()))
	if err != nil {
		return nil, err
	}
	output, err = aComponent.Handler.Call(ctx, sessionHandler)
	if err != nil {
		return output, err
	}
	err = anExecutor.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (s *Service) ensureOutputInstance(aComponent *repository.Component, response interface{}) interface{} {
	outputType := aComponent.Output.Type.Type()
	if outputType.Type().Kind() == reflect.Ptr {
		response = reflect.New(outputType.Type().Elem()).Interface()
	} else {
		response = reflect.New(outputType.Type()).Interface()
	}
	return response
}
