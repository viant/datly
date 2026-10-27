package bootstrap_test

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

type providerMaximum struct {
	Maximum *int `sqlx:"maximum"`
}
type providerInput struct {
	Tenant int
	Latest *providerMaximum
}
type providerRow struct {
	ID int `sqlx:"id"`
}
type providerOutput struct{ Rows []*providerRow }

func providerArtifact(t *testing.T) *bootstrap.Artifact {
	t.Helper()
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/providers", Name: "Records"},
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/records"}},
		Parameters: []*spec.Parameter{
			{Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}},
			{Name: "Latest", Source: spec.BindSource{Kind: "view", Name: "Latest"}, Cardinality: string(spec.CardinalityOne)},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		Views:    []*spec.View{{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/providers", Name: "Latest"}, Name: "Latest", Source: &spec.ViewSource{SQL: "SELECT MAX(id) AS maximum FROM records WHERE tenant=:Tenant"}}},
		RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE tenant=:Tenant AND id <= $Latest.Maximum ORDER BY id"}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[providerInput](), OutputType: reflect.TypeFor[providerOutput]()})
	require.NoError(t, err)
	return artifact
}

func TestCompiledReaderProvidersResolveNullableOncePerInvocation(t *testing.T) {
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
	require.Len(t, providers, 1)
	registered, err := artifact.Registration(registry.RegisteredComponent{Reader: reader, Providers: providers})
	require.NoError(t, err)
	var counts [2]atomic.Int64
	create := func(index int) *druntime.Runtime {
		runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered}, druntime.WithObservability(druntime.ObservabilityConfig{ReadingData: func(_ string, _ time.Duration, SQL string, rows int, args []any, err error) {
			if strings.Contains(strings.ToUpper(SQL), "MAX(") {
				require.NoError(t, err)
				counts[index].Add(1)
				t.Logf("maximum read rows=%d args=%v", rows, args)
			}
		}}))
		require.NoError(t, err)
		t.Cleanup(func() { _ = runtime.Shutdown(ctx) })
		return runtime
	}
	a, b := create(0), create(1)
	for _, tc := range []struct {
		runtime *druntime.Runtime
		tenant  int
		ids     []int
	}{
		{a, 7, []int{1, 2}}, {b, 8, []int{3}}, {a, 9, []int{}}, {a, 7, []int{1, 2}},
	} {
		result, err := tc.runtime.ExecuteRoute(ctx, http.MethodGet, "/records", testharness.NewRequest(http.MethodGet, "/records").WithQuery(url.Values{"tenant": {strconv.Itoa(tc.tenant)}}))
		require.NoError(t, err)
		rows := result.(*providerOutput).Rows
		ids := make([]int, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.ID)
		}
		require.Equal(t, tc.ids, ids, "independent query counts: %d/%d", counts[0].Load(), counts[1].Load())
	}
	require.EqualValues(t, 3, counts[0].Load())
	require.EqualValues(t, 1, counts[1].Load())
}

func TestCompiledReaderProvidersRejectMissingSQL(t *testing.T) {
	_, err := providerArtifact(t).ReaderCompilation().NewProviders(bootstrap.ReaderRuntimeConfig{})
	require.Error(t, err)
	var missing *bootstrap.CompiledReader
	_, err = missing.NewProviders(bootstrap.ReaderRuntimeConfig{})
	require.Error(t, err)
}
