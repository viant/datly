package selectorcompanion

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/state"
	provider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/logger"
	"reflect"
	"testing"
)

type optionalLogInput struct{ Logger logger.Logger }
type optionalLog struct{}

func (*optionalLog) Debug(string, ...any) {}
func (*optionalLog) Info(string, ...any)  {}
func (*optionalLog) Warn(string, ...any)  {}
func (*optionalLog) Error(string, ...any) {}
func TestPublicPreflightDelegatesOptionalLoggerToBindly(t *testing.T) {
	optional := false
	bindings := []bindly.BindingSpec{{Name: "Logger", Path: "Logger", Location: state.Location{Kind: "logger"}, Required: &optional}}
	plan := &BindingPlan{public: bindings}
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	public, err := injector.CompilePlan(reflect.TypeFor[optionalLogInput](), bindings...)
	require.NoError(t, err)
	trusted := &optionalLog{}
	for _, supplied := range []bool{false, true} {
		var providers []locator.Provider
		if supplied {
			providers = append(providers, provider.Static(handler.LoggerKey, trusted))
		}
		require.NoError(t, plan.ValidateProviders(providers))
		scoped, err := injector.ForScope(providers...)
		require.NoError(t, err)
		input := &optionalLogInput{}
		require.NoError(t, scoped.Bind(t.Context(), input, bindly.WithPlan(public)))
		if supplied {
			require.Same(t, trusted, input.Logger)
		} else {
			require.Nil(t, input.Logger)
		}
	}
}
