package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/testharness"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
)

type runtimeProviderScope []locator.Provider

func (s runtimeProviderScope) Providers() []locator.Provider { return s }

func newTestService(t *testing.T, bundle *route.Bundle, registered map[string]*registry.RegisteredComponent) *Runtime {
	t.Helper()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatalf("NewInjector() error = %v", err)
	}
	return &Runtime{
		bundle: bundle, registered: registered, injector: injector,
		invoker: handlerengine.New(),
	}
}

func executeTestRoute(t *testing.T, runtime *Runtime, ctx context.Context, request testharness.Request) (any, error) {
	t.Helper()
	pathParams, ok := runtime.MatchPathParams(request.HTTP.Method, request.HTTP.URL.Path)
	if !ok {
		pathParams = nil
	}
	request = request.WithPathParams(pathParams)
	scope, err := request.Scope()
	if err != nil {
		return nil, err
	}
	actual, execErr := runtime.executeRoute(ctx, request.HTTP.Method, request.HTTP.URL.Path, scope)
	return actual, errors.Join(execErr, scope.Close())
}
