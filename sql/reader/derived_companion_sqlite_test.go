package reader_test

import (
	"context"
	"github.com/stretchr/testify/require"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/derivedselector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	structjson "github.com/viant/structology/encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestDerivedOutputDeferredCompanionSQLite(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	c, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
	require.NoError(t, err)
	contract, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/derived-records"})
	require.True(t, ok)
	require.True(t, contract.SelectorPlan().DeferredPage())
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'a'),(2,'b')"))
	execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	app, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
	require.NoError(t, err)
	defer app.Shutdown(context.Background())
	for _, query := range []string{"?fields=id", "?fields=id,computed-total", "?fields=computed-total", "?fields=", "?fields=computed-total,unknown&page=bad"} {
		t.Run(query, func(t *testing.T) {
			scope, err := requestprovider.New(httptest.NewRequest("GET", "/derived-records"+query, nil))
			require.NoError(t, err)
			ctx := dexec.CaptureOutputSelection(t.Context())
			value, err := app.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/derived-records"}}, Providers: scope.Providers()})
			if query == "?fields=computed-total,unknown&page=bad" {
				require.ErrorContains(t, err, "unknown")
				require.NotContains(t, err.Error(), "page value")
				return
			}
			require.NoError(t, err)
			out := value.(*fixture.Output)
			require.Len(t, out.Data, 2)
			encoded, err := structjson.MarshalStandard(out, structjson.WithPathFieldExcluder(dexec.SelectedOutputFields(ctx, value)))
			require.NoError(t, err)
			if query == "?fields=computed-total" {
				require.JSONEq(t, `{"data":[{"computed-total":null},{"computed-total":null}]}`, string(encoded))
			}
		})
	}
}
