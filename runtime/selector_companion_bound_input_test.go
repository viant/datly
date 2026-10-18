package runtime_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	rh "github.com/viant/datly/runtime/handler"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/structology"
	xh "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/state"
)

type forbiddenBoundQuery struct{ locates int }

func (p *forbiddenBoundQuery) Kind() string  { return "query" }
func (p *forbiddenBoundQuery) Priority() int { return 0 }
func (p *forbiddenBoundQuery) Locate(s *structology.State) locator.Locator {
	p.locates++
	return hp.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) {
		return nil, false, fmt.Errorf("bound reader acquired hostile parent query")
	}).Locate(s)
}

func TestSelectorCompanionTrustedBoundReaderSQLite(t *testing.T) {
	ctx := t.Context()
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	component.RootView.Source.SQL = "SELECT id,label FROM records WHERE id = $ID"
	for _, p := range component.Parameters {
		if p.Name == "ID" {
			required := true
			p.Required = &required
			p.ErrorStatusCode = 400
		}
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
	require.NoError(t, err)
	require.NotNil(t, artifact.Input)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"))
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/selector-records"}}
	var childInput *fixture.Input
	var childProviders []locator.Provider
	var childOutput *fixture.Output
	parentSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "BoundParent"}, Name: "BoundParent", Routes: []*spec.Route{{Method: "POST", Path: "/bound-parent", Handler: "fixture.Parent"}}}
	parentArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parentSpec, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[struct{}]()})
	require.NoError(t, err)
	parent := &registry.RegisteredComponent{Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeFor[struct{}](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
		value, ok, err := inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("child invoker missing")
		}
		actual, err := value.(dexec.ComponentInvoker).InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: childInput, Providers: childProviders})
		if err == nil {
			childOutput = actual.(*fixture.Output)
		}
		return &struct{}{}, err
	})}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}, parent})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	parentTarget := dexec.ComponentTarget{Component: parentSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/bound-parent"}}
	invoke := func(id int, providers []locator.Provider, parentProviders []locator.Provider) error {
		childInput = &fixture.Input{ID: id, Has: &fixture.InputHas{ID: true}}
		childProviders = providers
		childOutput = nil
		_, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: parentTarget, Input: &struct{}{}, Providers: parentProviders})
		require.Equal(t, id, childInput.ID)
		require.True(t, childInput.Has.ID)
		return err
	}
	require.NoError(t, invoke(1, nil, nil))
	require.Equal(t, []*fixture.Row{{ID: 1, Label: "one"}}, childOutput.Data)
	hostile := &forbiddenBoundQuery{}
	require.NoError(t, invoke(2, nil, []locator.Provider{hostile}))
	require.Equal(t, []*fixture.Row{{ID: 2, Label: "two"}}, childOutput.Data)
	require.Zero(t, hostile.locates)
	selectors := hp.Static(xh.SelectorsKey, state.Selectors{&state.NamedSelector{Name: "rows", Selector: state.Selector{Fields: []string{"label"}}}})
	require.NoError(t, invoke(1, []locator.Provider{selectors}, []locator.Provider{hostile}))
	require.Equal(t, []*fixture.Row{{Label: "one"}}, childOutput.Data)
	require.Zero(t, hostile.locates)
	require.NoError(t, invoke(2, nil, nil))
	require.Equal(t, []*fixture.Row{{ID: 2, Label: "two"}}, childOutput.Data)
	childInput = &fixture.Input{ID: 1, Has: &fixture.InputHas{}}
	childProviders = nil
	_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: parentTarget, Input: &struct{}{}})
	require.ErrorContains(t, err, "required bound query")
	// Ordinary HTTP and ExtraInput still bind their own side selectors.
	handler := gateway.NewHandler(rt, nil, "bound-companion")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/selector-records?id=1&fields=label", nil))
	require.Equal(t, 200, recorder.Code)
	require.Contains(t, recorder.Body.String(), "one")
	query, err := requestprovider.New(httptest.NewRequest("GET", "/selector-records?fields=label", nil))
	require.NoError(t, err)
	defer query.Close()
	actual, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(&fixture.Input{ID: 2, Has: &fixture.InputHas{ID: true}}), Providers: query.Providers()})
	require.NoError(t, err)
	require.Equal(t, []*fixture.Row{{Label: "two"}}, actual.(*fixture.Output).Data)
}
