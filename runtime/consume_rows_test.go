package runtime_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	de "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	dr "github.com/viant/datly/runtime"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	ds "github.com/viant/datly/sql"
	xh "github.com/viant/xdatly/handler"
)

func TestPublicInvokerConsumesGeneratedReaderRows(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	component.RootView.Source.SQL = "SELECT id,label FROM records WHERE id >= $ID ORDER BY id"
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
	require.NoError(t, err)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"))
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &ds.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	parentSpec := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ConsumeParent"}, Name: "ConsumeParent", Routes: []*spec.Route{{Method: "POST", Path: "/consume-parent", Handler: "fixture.Parent"}}}
	parentArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parentSpec, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[struct{}]()})
	require.NoError(t, err)
	var ids []int
	parent := &registry.RegisteredComponent{Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeFor[struct{}](), Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
		capability, ok, err := inv.Binder.Lookup(ctx, xh.ComponentInvokerKey)
		require.NoError(t, err)
		require.True(t, ok)
		request := xh.ComponentRequest{Target: xh.ComponentTarget{Scope: component.Key.Scope, Name: component.Key.Name, Method: "GET", Path: "/selector-records"}, Input: &fixture.Input{ID: 1, Has: &fixture.InputHas{ID: true}}, Rows: xh.ConsumeRows(func(_ context.Context, row *fixture.Row) error { ids = append(ids, row.ID); return nil })}
		actual, err := capability.(xh.ComponentInvoker).Invoke(ctx, request)
		require.NoError(t, err)
		require.Empty(t, actual.(*fixture.Output).Data)
		request.Rows = nil
		ordinary, err := capability.(xh.ComponentInvoker).Invoke(ctx, request)
		require.NoError(t, err)
		require.Len(t, ordinary.(*fixture.Output).Data, 2)
		return &struct{}{}, nil
	})}
	rt, err := dr.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}, parent})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	_, err = rt.InvokeComponent(t.Context(), de.ComponentRequest{Target: de.ComponentTarget{Component: parentSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/consume-parent"}}, Input: &struct{}{}})
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, ids)
}
