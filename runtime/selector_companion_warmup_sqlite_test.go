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
