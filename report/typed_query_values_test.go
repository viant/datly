package report

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
)

func TestTypedReportQueryListsUseCanonicalDecoder(t *testing.T) {
	type has struct{ Statuses bool }
	type input struct {
		Statuses []string `parameter:",kind=query,in=statuses"`
		Has      *has     `setMarker:"true" json:"-"`
	}
	compiled, err := handlercompiler.New(handlercompiler.Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/records"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	require.NoError(t, err)
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/records"})
	require.True(t, ok)
	for _, tc := range []struct {
		name  string
		value []string
		found bool
	}{
		{"ordinary", []string{"queued"}, true},
		{"comma is one typed item", []string{"queued,pending"}, true},
		{"explicit empty", []string{}, true},
		{"absent", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]typedValue{}
			if tc.found {
				values["statuses"] = typedValue{typeOf: reflect.TypeFor[[]string](), value: tc.value, owns: true, found: true}
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(newTypedValueProvider("query", values)))
			require.NoError(t, err)
			var out input
			require.NoError(t, injector.Bind(context.Background(), &out, bindly.WithPlan(route.Plan())))
			require.Equal(t, tc.value, out.Statuses)
			require.Equal(t, tc.found, out.Has != nil && out.Has.Statuses)
		})
	}
}
