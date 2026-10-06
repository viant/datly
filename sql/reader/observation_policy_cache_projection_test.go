package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader/collector"
	"github.com/viant/sqlx/io/read/cache"
	xexec "github.com/viant/xdatly/exec"
)

func TestObservationSourceOwnerWarmupCacheAndExecutedProjectionSQLite(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "module.cache", Name: "Records"}, RootView: &spec.View{Name: "records", Namespace: "r", Source: &spec.ViewSource{SQL: "SELECT id FROM records ORDER BY id"}}}
	view := data.FromComponent(component)
	view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{}}
	graph, err := collector.Compile(view, map[*data.View]reflect.Type{view: reflect.TypeFor[row]()})
	require.NoError(t, err)
	plan, err := NewPlan(PlanConfig{RootView: view, ViewIndex: NewViewIndex(component, view), Collection: graph, OutputViewField: "Rows"})
	require.NoError(t, err)
	cacheService, err := (cacheconfig.Config{Identity: "Records/root", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	e, err := NewExecution(Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{view: cacheService}})
	require.NoError(t, err)
	id, err := view.Spec.Identity()
	require.NoError(t, err)
	policy := &observability.Policy{Views: []observability.ViewObservation{{Component: component.Key, ViewIdentity: id, Diagnostic: "records#", Operation: &observability.OperationDescriptor{Name: "platform.cache.records", Location: "platform/cache", Description: "records performance", Provider: observability.Source11}}}}
	owner := observability.NewRecorder(nil, observability.WithPolicy(policy))
	require.NoError(t, owner.ValidateTargets(e.ObservationTargets()))
	e = e.WithRecorder(owner).(*Execution)
	// PrepareQuery itself has no timed read. The executed projection retains the
	// resolved operation and diagnostic when constructing its detached session.
	ec := xexec.New()
	query, err := e.PrepareQuery(xexec.WithContext(ctx, ec), &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.Empty(t, owner.Values("platform.cache.records"))
	require.Empty(t, ec.Metrics)
	value, err := query.Projection.ReadProjection(xexec.WithContext(ctx, ec), dexec.ProjectionRequest{SQL: query.SQL, Args: query.Args, RowType: reflect.TypeFor[row]()})
	require.NoError(t, err)
	require.Len(t, value.([]*row), 2)
	require.Len(t, ec.Metrics, 1)
	require.Equal(t, "records#", ec.Metrics[0].View)
	warmCtx := xexec.WithContext(ctx, xexec.New())
	groups, err := e.Warmup(warmCtx, dexec.ReaderWarmupInvocation{Input: &struct{}{}})
	require.NoError(t, err)
	require.Equal(t, 1, groups)
	require.NoError(t, db.ExecStatements(ctx, "DROP TABLE records"))
	readCtx := xexec.WithContext(ctx, xexec.New())
	actual, err := e.Read(readCtx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []row{{1}, {2}}, actual.(*output).Rows)
	metrics := xexec.GetContext(readCtx).Metrics
	require.Len(t, metrics, 1)
	require.Equal(t, "records#", metrics[0].View)
	require.True(t, metrics[0].Executions[0].CacheStats.FoundWarmup)
	values := owner.Values("platform.cache.records")
	require.Len(t, values, 11)
	require.EqualValues(t, 3, values["Success"])
	require.EqualValues(t, 1, values["cache:hit"])
	require.EqualValues(t, 1, values["cache:warmup_hit"])
	require.Zero(t, values["Pending"])
	require.NotContains(t, values, "cache:created")
	require.Empty(t, owner.Values(component.Key.String()+"/records"))
	// The captured schema is fixed at operation creation; publication skips the
	// unavailable extensions while successful cache execution remains observable.
}

func TestObservationRecorderRebindingPreservesNativeDefaultsSQLite(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Rows []row }
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "module.rebind", Name: "Records"}, RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}}
	view := data.FromComponent(component)
	graph, err := collector.Compile(view, map[*data.View]reflect.Type{view: reflect.TypeFor[row]()})
	require.NoError(t, err)
	plan, err := NewPlan(PlanConfig{RootView: view, ViewIndex: NewViewIndex(component, view), Collection: graph, OutputViewField: "Rows"})
	require.NoError(t, err)
	e, err := NewExecution(Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[output](), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	id, err := view.Spec.Identity()
	require.NoError(t, err)
	p := &observability.Policy{Views: []observability.ViewObservation{{Component: component.Key, ViewIdentity: id, Diagnostic: "records#", Operation: &observability.OperationDescriptor{Name: "platform.records", Location: "platform", Description: "records performance", Provider: "invalid"}}}}
	invalid := e.WithRecorder(observability.NewRecorder(nil, observability.WithPolicy(p))).(*Execution)
	_, err = invalid.Read(ctx, &struct{}{}, nil, nil)
	require.ErrorContains(t, err, "invalid observation operation")
	p.Views[0].Operation.Provider = observability.Source11
	owner := observability.NewRecorder(nil, observability.WithPolicy(p))
	rebound := invalid.WithRecorder(owner).(*Execution)
	ec := xexec.New()
	actual, err := rebound.Read(xexec.WithContext(ctx, ec), &struct{}{}, nil, nil)
	require.NoError(t, err, "new owner replaces prior resolution failure")
	require.Equal(t, []row{{7}}, actual.(*output).Rows)
	require.Equal(t, "records#", ec.Metrics[0].View)
	require.EqualValues(t, 1, owner.Values("platform.records")["Success"])
	native := invalid.WithRecorder(nil).(*Execution)
	ec = xexec.New()
	actual, err = native.Read(xexec.WithContext(ctx, ec), &struct{}{}, nil, nil)
	require.NoError(t, err, "nil recorder retains session-local native capture")
	require.Equal(t, []row{{7}}, actual.(*output).Rows)
	require.Equal(t, "records", ec.Metrics[0].View)
	require.EqualValues(t, 1, owner.Values("platform.records")["Success"], "detached native read does not capture into prior source owner")
}
