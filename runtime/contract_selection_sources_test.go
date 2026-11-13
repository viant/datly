package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/handler/provider"
	xresponse "github.com/viant/xdatly/response"
)

type selectionTestScope []locator.Provider

func (s selectionTestScope) Providers() []locator.Provider { return s }

type selectionOpaque struct {
	Value string
	Hook  func()
}

func (v *selectionOpaque) MarshalJSON() ([]byte, error) { return json.Marshal(v.Value) }
func TestContractSelectionOpaqueSourceDisablesReuse(t *testing.T) {
	type input struct{ Value *selectionOpaque }
	value := &selectionOpaque{Value: "first", Hook: func() {}}
	scope := selectionTestScope{provider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return value, true, nil })}
	injector, err := bindly.NewInjector(bindly.WithProviders(scope...))
	require.NoError(t, err)
	bindings := []bindly.BindingSpec{{Path: "Value", Location: state.Location{Kind: "header", In: "value"}}}
	plan, err := injector.CompilePlan(reflect.TypeFor[input](), bindings...)
	require.NoError(t, err)
	selection, err := plan.Replay("Value")
	require.NoError(t, err)
	receipt := newContractBindings(nil)
	binder := &contractStateBinder{Binder: injector, scope: &contractSourceScope{protocol: scope}, values: receipt}
	var actual input
	require.NoError(t, binder.BindContractState(t.Context(), &actual, plan, bindings, selection))
	require.Equal(t, "first", actual.Value.Value)
	ok, err := receipt.matches(t.Context(), &contractSourceScope{protocol: scope})
	require.NoError(t, err)
	require.False(t, ok)
	receipt.close()
	require.Empty(t, receipt.selections)
	require.Empty(t, receipt.values)
}
func TestContractSelectionRejectsSourcesChangingDuringBinding(t *testing.T) {
	type input struct{ Value string }
	reads := 0
	scope := selectionTestScope{provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) {
		reads++
		if reads == 1 {
			return "first", true, nil
		}
		return "second", true, nil
	})}
	injector, err := bindly.NewInjector(bindly.WithProviders(scope...))
	require.NoError(t, err)
	bindings := []bindly.BindingSpec{{Path: "Value", Location: state.Location{Kind: "query", In: "value"}}}
	plan, err := injector.CompilePlan(reflect.TypeFor[input](), bindings...)
	require.NoError(t, err)
	selection, err := plan.Replay("Value")
	require.NoError(t, err)
	receipt := newContractBindings(nil)
	binder := &contractStateBinder{Binder: injector, scope: &contractSourceScope{protocol: scope}, values: receipt}
	var actual input
	err = binder.BindContractState(t.Context(), &actual, plan, bindings, selection)
	require.EqualValues(t, 409, xresponse.ErrorStatusCode(err, 500))
	require.Empty(t, receipt.selections)
}
func TestContractSelectionCapturePreservesSourceFailureCauses(t *testing.T) {
	type input struct{ Value string }
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("unavailable source")} {
		t.Run(cause.Error(), func(t *testing.T) {
			scope := selectionTestScope{provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, cause })}
			injector, err := bindly.NewInjector(bindly.WithProviders(scope...))
			require.NoError(t, err)
			bindings := []bindly.BindingSpec{{Path: "Value", Location: state.Location{Kind: "query", In: "value"}}}
			plan, err := injector.CompilePlan(reflect.TypeFor[input](), bindings...)
			require.NoError(t, err)
			selection, err := plan.Replay("Value")
			require.NoError(t, err)
			receipt := newContractBindings(nil)
			binder := &contractStateBinder{Binder: injector, scope: &contractSourceScope{protocol: scope}, values: receipt}
			var actual input
			require.ErrorIs(t, binder.BindContractState(t.Context(), &actual, plan, bindings, selection), cause)
			require.Empty(t, receipt.selections)
		})
	}
}
