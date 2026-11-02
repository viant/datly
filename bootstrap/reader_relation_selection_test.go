package bootstrap_test

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

func TestReaderRelationSelectionDoesNotReachPrerequisiteViews(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tenant INTEGER)", "INSERT INTO records VALUES(1,7),(2,7),(3,8)"))
	artifact := providerArtifact(t)
	compiled := artifact.ReaderCompilation()
	config := bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}}
	reader, err := compiled.NewExecution(config)
	require.NoError(t, err)
	providers, err := compiled.NewProviders(config)
	require.NoError(t, err)
	registered, err := artifact.Registration(registry.RegisteredComponent{Reader: reader, Providers: providers})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { _ = runtime.Shutdown(ctx) })
	target := exec.ComponentTarget{Component: registered.Component.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/records"}}
	for _, tc := range []struct {
		tenant int
		ids    []int
	}{{7, []int{1, 2}}, {8, []int{3}}, {9, []int{}}, {7, []int{1, 2}}} {
		request := testharness.NewRequest(http.MethodGet, "/records").WithQuery(url.Values{"tenant": {strconv.Itoa(tc.tenant)}})
		result, err := runtime.InvokeComponent(ctx, exec.ComponentRequest{
			Target: target, Providers: request.Providers(),
			ReaderOptions: &exec.ReaderOptions{Relations: exec.NewRelationSelection(target, "Rows")},
		})
		require.NoError(t, err)
		var ids []int
		for _, row := range result.(*providerOutput).Rows {
			ids = append(ids, row.ID)
		}
		require.Equal(t, tc.ids, append([]int{}, ids...), "prerequisite tenant scope must remain authoritative")
	}
}
