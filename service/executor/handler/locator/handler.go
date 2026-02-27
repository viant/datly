package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository/handler"
	ehandler "github.com/viant/datly/service/executor/handler"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"reflect"
)

type Handler struct {
	options *locator.Options
	types   []*state.Type
}

func (v *Handler) Names() []string {
	return nil
}

func (v *Handler) Value(ctx context.Context, _ reflect.Type, name string) (interface{}, bool, error) {
	resource := v.options.Resource
	if resource == nil {
		return nil, false, fmt.Errorf("failed to lookup handler resource: %v", name)
	}
	parameter := v.options.LookupParameters(name)
	if parameter == nil {
		return nil, false, fmt.Errorf("failed to lookup handler parameter: %v", name)
	}
	anHandler := handler.Handler{}
	anHandler.Type = parameter.Handler.Name
	anHandler.Output = parameter.Schema.Type()
	anHandler.Arguments = parameter.Handler.Args
	resource.SetTypes(extension.Config.Types)
	err := anHandler.Init(ctx, resource)
	if err != nil {
		return nil, false, fmt.Errorf("failed to initialize handler: %w", err)
	}
	aView := &view.View{}
	aView.SetResource(resource)
	if len(resource.Connectors) > 0 {
		aView.Connector = resource.Connectors[0]
	}
	aSession := session.Context(ctx)
	anExecutor := ehandler.NewExecutor(aView, aSession)

	handlerSession, err := anExecutor.NewHandlerSession(ctx, ehandler.WithTypes(v.types...), ehandler.WithAuth(aSession.Auth()))
	if err != nil {
		return nil, false, fmt.Errorf("failed to create handler session: %w", err)
	}
	result, err := anHandler.Call(ctx, handlerSession)
	return result, err == nil, err
}

// NewHandler returns Handler locator
func NewHandler(opts ...locator.Option) (kind.Locator, error) {
	ret := &Handler{options: locator.NewOptions(opts)}
	return ret, nil
}

func init() {
	locator.Register(state.KindHandler, NewHandler)
}
