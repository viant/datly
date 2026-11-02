package runtime_test

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/finalizerselection"
	dexec "github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	rh "github.com/viant/datly/runtime/handler"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	xh "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/state"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestFinalizerEffectiveSelectionHTTPAndTypedSQLite(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
	require.NoError(t, err)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE parents(id INTEGER,name TEXT)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO parents VALUES(1,'empty')"))
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	var bindProviders []locator.Provider
	var bound fixture.Output
	parentSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "SelectionBindingParent"}, Name: "SelectionBindingParent", Routes: []*spec.Route{{Method: "POST", Path: "/selection-binding-parent", Handler: "fixture.Bind"}}}
	parentArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parentSpec, InputType: reflect.TypeFor[finalizerSelectionBindingInput](), OutputType: reflect.TypeFor[struct{}]()})
	require.NoError(t, err)
	parent := &druntime.RegisteredComponent{Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeFor[struct{}](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
		input := inv.Input.(*finalizerSelectionBindingInput)
		bound = fixture.Output{}
		_, err := input.Invoker.(xh.ComponentResponseBinder).BindResponse(ctx, xh.ComponentRequest{Target: xh.ComponentTarget{Scope: component.Key.Scope, Name: component.Key.Name, Method: "GET", Path: "/finalizer-selection"}, Providers: bindProviders}, &bound)
		return &struct{}{}, err
	})}
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}, parent})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/finalizer-selection"}}
	type useCase struct {
		description, query string
		selected           bool
	}
	for _, tc := range []useCase{{"default", "", true}, {"excluded", "?fields=name", false}, {"Go relation", "?fields=Children", true}, {"public relation", "?fields=entries", true}} {
		t.Run(tc.description, func(t *testing.T) {
			scope, err := requestprovider.New(httptest.NewRequest("GET", "/finalizer-selection"+tc.query, nil))
			require.NoError(t, err)
			defer scope.Close()
			result, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
			require.NoError(t, err)
			out := result.(*fixture.Output)
			require.NotNil(t, out.Selected)
			require.Equal(t, tc.selected, *out.Selected)
			require.Empty(t, out.Data[0].Children)
			bindProviders = scope.Providers()
			_, err = rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: parentSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/selection-binding-parent"}}})
			require.NoError(t, err, "BindResponse finalizer must see its own selection")
			require.NotNil(t, bound.Selected)
			require.Equal(t, tc.selected, *bound.Selected)

			rec := httptest.NewRecorder()
			gateway.NewHandler(rt, nil, "selection").ServeHTTP(rec, httptest.NewRequest("GET", "/finalizer-selection"+tc.query, nil))
			require.Equal(t, 200, rec.Code, rec.Body.String())
			if tc.selected {
				require.Contains(t, rec.Body.String(), `"selected":true`)
			} else {
				require.Contains(t, rec.Body.String(), `"selected":false`)
			}
		})
	}
	scope, err := requestprovider.New(httptest.NewRequest("GET", "/finalizer-selection?fields=name", nil))
	require.NoError(t, err)
	defer scope.Close()
	providers := append(scope.Providers(), hp.Static(xh.SelectorsKey, state.Selectors{&state.NamedSelector{Name: "parents", Selector: state.Selector{Fields: []string{"Children"}}}}))
	result, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: providers})
	require.NoError(t, err)
	require.True(t, *result.(*fixture.Output).Selected, "explicit selector must override transport fields")
	result, err = rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Input: &fixture.Input{}})
	require.NoError(t, err)
	require.True(t, *result.(*fixture.Output).Selected)
}

type finalizerSelectionBindingInput struct {
	Invoker xh.ComponentInvoker `parameter:"Invoker,kind=component_invoker,required"`
}
