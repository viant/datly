package runtime

import (
	requestprovider "github.com/viant/bindly/provider/request"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	childfixture "github.com/viant/datly/runtime/testdata/selectorchildwarmup"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

func TestSelectorCompanionRequiredIndexWarmupSQLite(t *testing.T) {
	ctx := t.Context()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"))
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	for _, p := range component.Parameters {
		if p.Name == "ID" {
			required := true
			p.Required = &required
		}
	}
	component.Settings = &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "selector-records", Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{IndexColumn: "id", IndexParameter: "ID"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
	require.NoError(t, err)
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
	require.NoError(t, err)
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/selector-records"}}
	operation, err := rt.NewWarmup(target)
	require.NoError(t, err)
	prepared, err := operation.Prepare(ctx)
	require.NoError(t, err)
	count, err := prepared.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	route, ok := artifact.Input.ForRoute(target.Route)
	require.True(t, ok)
	require.True(t, *route.Fields()[0].Binding().Required)
	require.NoError(t, db.ExecStatements(ctx, "DROP TABLE records"))
	scope, err := requestprovider.New(httptest.NewRequest("GET", "/selector-records?id=1", nil))
	require.NoError(t, err)
	defer scope.Close()
	actual, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
	require.NoError(t, err)
	require.Equal(t, []*fixture.Row{{ID: 1, Label: "one"}}, actual.(*fixture.Output).Data)
}

func TestSelectorCompanionChildRequiredIndexWarmupSQLite(t *testing.T) {
	ctx := t.Context()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE records(id INTEGER,label TEXT)",
		"INSERT INTO records VALUES(1,'one'),(2,'two')",
		"CREATE TABLE children(id INTEGER,parent_id INTEGER,label TEXT)",
		"INSERT INTO children VALUES(11,1,'child one'),(22,2,'child two')"))
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[childfixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := found.Components[0].Resolve(reflect.TypeFor[childfixture.Input](), reflect.TypeFor[childfixture.Output]())
	require.NoError(t, err)
	for _, parameter := range component.Parameters {
		if parameter.Name == "ID" {
			required := true
			parameter.Required = &required
		}
	}
	component.RootView.Source.SQL = "SELECT id,label FROM records WHERE id=:ID"
	component.RootView.Relations = []*spec.Relation{{
		Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany,
		On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}},
		View: &spec.View{Name: "children", Source: &spec.ViewSource{
			SQL:      "SELECT id,parent_id,label FROM children WHERE $COLUMN_IN",
			Bindings: &spec.ViewBindings{CacheName: "child-cache", CacheWarmup: "child-warmup"},
		}},
	}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[childfixture.Input](), OutputType: reflect.TypeFor[childfixture.Output](), DirectViewField: "Data"})
	require.NoError(t, err)
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{
		SQL: &dsql.SQLComponent{DB: db.DB},
		CacheSettings: map[string]*spec.CacheSettings{
			"child-cache":  {Enabled: true, Location: t.TempDir(), TTL: "1m"},
			"child-warmup": {Warmup: &spec.CacheWarmupSettings{IndexColumn: "parent_id", IndexParameter: "ID"}},
		},
	})
	require.NoError(t, err)
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[childfixture.Output](), Reader: execution}})
	require.NoError(t, err)
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/selector-records"}}
	operation, err := rt.NewWarmup(target)
	require.NoError(t, err)
	prepared, err := operation.Prepare(ctx)
	require.NoError(t, err)
	count, err := prepared.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	route, ok := artifact.Input.ForRoute(target.Route)
	require.True(t, ok)
	require.True(t, *route.Fields()[0].Binding().Required)
	require.NoError(t, db.ExecStatements(ctx, "DROP TABLE children"))
	scope, err := requestprovider.New(httptest.NewRequest("GET", "/selector-records?id=1", nil))
	require.NoError(t, err)
	defer scope.Close()
	actual, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
	require.NoError(t, err)
	require.Equal(t, []*childfixture.Row{{ID: 1, Label: "one", Children: []*childfixture.Child{{ID: 11, ParentID: 1, Label: "child one"}}}}, actual.(*childfixture.Output).Data)
	_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target})
	require.ErrorContains(t, err, "ID is required")
}
