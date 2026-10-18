package selectorcompanion

import (
	"context"
	"net/url"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/bindly/state"
	provider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
)

func TestSideSelectorDefaultsUseNativeBindingWithoutParentQuery(t *testing.T) {
	type public struct{}
	type side struct {
		Page  int
		Limit int
	}
	page := &spec.Parameter{Name: "Page", QuerySelector: &spec.QuerySelectorBinding{View: "Rows", Property: spec.SelectorPropertyPage}}
	limit := &spec.Parameter{Name: "Limit", QuerySelector: &spec.QuerySelectorBinding{View: "Rows", Property: spec.SelectorPropertyLimit}}
	component := &spec.Component{Parameters: []*spec.Parameter{page, limit}, RootView: &spec.View{Name: "Rows", Selector: &spec.Selector{NoLimit: true}}}
	bindings := []bindly.BindingSpec{
		{Path: "Page", Location: state.Location{Kind: "query", In: "page"}, DefaultValue: "2", Extension: page},
		{Path: "Limit", Location: state.Location{Kind: "query", In: "limit"}, DefaultValue: "2", Extension: limit},
	}
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	publicPlan, err := injector.CompilePlan(reflect.TypeFor[public](), make([]bindly.BindingSpec, 0)...)
	require.NoError(t, err)
	sidePlan, err := injector.CompilePlan(reflect.TypeFor[side](), bindings...)
	require.NoError(t, err)
	plan, err := NewBindingPlan(component, &Selection{Type: reflect.TypeFor[side](), Parameters: component.Parameters}, reflect.TypeFor[public](), publicPlan, sidePlan, nil, bindings)
	require.NoError(t, err)
	require.False(t, plan.HasPageSources(), "defaulted Page retains typed binding")
	parentQuery := provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) { return "99", true, nil })
	parent, err := bindly.NewInjector(bindly.WithProviders(parentQuery))
	require.NoError(t, err)
	unrelated := provider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) { return nil, false, nil })
	value, err := plan.Bind(context.Background(), parent, &public{}, unrelated)
	require.NoError(t, err)
	require.Equal(t, &side{Page: 2, Limit: 2}, value)
	query := url.Values{"page": {"3"}, "limit": {"1"}}
	native := requestprovider.NewValues(requestprovider.WithQuery(query))
	scoped, err := parent.ForScope(native.Query())
	require.NoError(t, err)
	value, err = plan.Bind(context.Background(), scoped, &public{}, native.Query())
	require.NoError(t, err)
	require.Equal(t, &side{Page: 3, Limit: 1}, value)
	native = requestprovider.NewValues(requestprovider.WithQuery(url.Values{"page": {"bad"}}))
	scoped, err = parent.ForScope(native.Query())
	require.NoError(t, err)
	_, err = plan.Bind(context.Background(), scoped, &public{}, native.Query())
	require.Error(t, err)
}
