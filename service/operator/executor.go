package operator

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/executor/handler"
	xhandler "github.com/viant/xdatly/handler"

	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view/state/kind/locator"
)

// HandlerSession returns a handler session
func (s *Service) HandlerSession(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (xhandler.Session, error) {
	anExecutor := handler.NewExecutor(aComponent.View, aSession)
	return anExecutor.NewHandlerSession(ctx, handler.WithTypes(aComponent.Types()...))
}

func (s *Service) execute(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
	anExecutor := handler.NewExecutor(aComponent.View, aSession)
	if aComponent.Handler != nil {
		aSession.SetView(aComponent.View)
		sessionHandler, err := anExecutor.NewHandlerSession(ctx,
			handler.WithTypes(aComponent.Types()...))
		if err != nil {
			return nil, err
		}
		response, err := aComponent.Handler.Call(ctx, sessionHandler)
		if err != nil {
			return nil, err
		}
		if err = anExecutor.Execute(ctx); err != nil {
			return nil, err
		}
		return response, nil
	}
	executorSession, err := anExecutor.ExpandAndExecute(ctx)
	if err != nil {
		return nil, err
	}
	var responseValue interface{}
	if aComponent.Output.ResponseBody == nil {
		return responseValue, nil
	}
	if stateType := aComponent.Output.Type.Type(); stateType != nil && stateType.IsDefined() {
		responseState := aComponent.Output.Type.Type().NewState()
		statelet := executorSession.Session.State().Lookup(executorSession.View)

		status := contract.StatusSuccess(executorSession.TemplateState)
		if err := aSession.SetState(ctx, aComponent.Output.Type.Parameters, responseState, aSession.Indirect(true,
			locator.WithCustomOption(&status),
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
