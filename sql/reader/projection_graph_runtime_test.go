package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/testutil/sqlfault"
	"strings"
)

type projectionLabel struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}
type projectionHookLabel projectionLabel

func (*projectionHookLabel) OnFetch(context.Context) error {
	return fmt.Errorf("label redaction must not be bypassed")
}

type projectionParent struct {
	ID    int              `sqlx:"id"`
	Total float64          `sqlx:"total"`
	Label *projectionLabel `sqlx:"-"`
}

func (*projectionParent) OnFetch(context.Context) error { return fmt.Errorf("root hook must not run") }
func (*projectionParent) OnRelation(context.Context)    { panic("root hook must not run") }

type projectionHookParent struct {
	ID    int                  `sqlx:"id"`
	Total float64              `sqlx:"total"`
	Label *projectionHookLabel `sqlx:"-"`
}
type projectionInput struct{ Tenant string }
type projectionContextKey struct{}
type projectionMissingAuthorityKey struct{}

func TestProjectionGraphNativeBatchBindingCancellationAndHooks(t *testing.T) {
	ctx := context.WithValue(context.Background(), projectionContextKey{}, "bound-source")
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE facts(id INTEGER,tenant TEXT,amount REAL)", "INSERT INTO facts VALUES(1,'a',10),(2,'a',20),(3,'a',30),(4,'a',40),(5,'a',50),(6,'b',900)", "CREATE TABLE labels(id INTEGER,tenant TEXT,name TEXT)", "INSERT INTO labels VALUES(1,'a','one'),(2,'a','two'),(3,'a','three'),(4,'a','four'),(5,'a','five'),(1,'b','foreign')"))
	var calls atomic.Int32
	var cancelRead context.CancelFunc
	connection := db.FaultDB(t, func(queryCtx context.Context, call sqlfault.Call) error {
		if call.Phase == "query" && strings.Contains(call.SQL, "FROM labels") {
			calls.Add(1)
			if queryCtx.Value(projectionContextKey{}) != "bound-source" {
				return fmt.Errorf("bound source context was lost")
			}
			if queryCtx.Value(projectionMissingAuthorityKey{}) != nil {
				return fmt.Errorf("caller supplied authority absent from prepared source")
			}
			if cancelRead != nil {
				cancelRead()
				<-queryCtx.Done()
				return queryCtx.Err()
			}
		}
		return nil
	})
	grouped := true
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}}}, RootView: &spec.View{Name: "facts", Groupable: &grouped, Source: &spec.ViewSource{SQL: "SELECT id,SUM(amount) AS total FROM facts WHERE tenant=:tenant GROUP BY id"}, Relations: []*spec.Relation{{Name: "Label", Holder: "Label", Cardinality: spec.CardinalityOne, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "id"}}, View: &spec.View{Name: "labels", BatchSize: 2, Source: &spec.ViewSource{SQL: "SELECT id,name FROM labels WHERE tenant=:tenant AND $COLUMN_IN"}}}}}}
	typ := reflect.TypeFor[[]*projectionParent]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[projectionInput](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	sourceSQL := &dsql.SQLComponent{DB: connection}
	dialect, err := sourceSQL.Dialect(ctx)
	require.NoError(t, err)
	previousBudget := dialect.MaxPlaceholders
	defer func() { dialect.MaxPlaceholders = previousBudget }()
	dialect.MaxPlaceholders = 3 // one tenant binding plus each native two-key batch
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[projectionInput](), OutputType: typ, Plan: plan, SQL: sourceSQL})
	require.NoError(t, err)
	prepared, err := execution.PrepareQuery(ctx, &projectionInput{Tenant: "a"}, nil, func(name string) (any, bool, error) { return "a", name == "tenant", nil })
	require.NoError(t, err)
	type projected struct {
		Key   int     `sqlx:"key" json:"key"`
		Total float64 `sqlx:"total" json:"total"`
	}
	request := dexec.ProjectionGraphRequest{RowType: reflect.TypeFor[projected](), Relations: []dexec.ProjectionRelation{{Name: "Label", Output: "label", Keys: []dexec.ProjectionKey{{Source: "id", Output: "key"}}}}}
	graph, err := prepared.Projection.(dexec.ProjectionGraphReader).PrepareProjectionGraph(ctx, request)
	require.NoError(t, err)
	rows, err := prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: "SELECT id AS key,total FROM (" + prepared.SQL + ") q ORDER BY key", Args: prepared.Args, RowType: graph.RowType()})
	require.NoError(t, err)
	callerContext := context.WithValue(context.WithValue(context.Background(), projectionContextKey{}, "caller-overwrite"), projectionMissingAuthorityKey{}, "foreign-tenant-header")
	loaded, err := graph.Load(callerContext, rows)
	require.NoError(t, err)
	require.EqualValues(t, 3, calls.Load(), "native BatchSize=2 must load five keys in three batches")
	value := reflect.ValueOf(loaded)
	require.Equal(t, 5, value.Len())
	for i := 0; i < value.Len(); i++ {
		label := value.Index(i).Elem().FieldByName("Label").Interface().(*projectionLabel)
		require.Equal(t, i+1, label.ID)
		require.NotEqual(t, "foreign", label.Name)
	}
	dialect.MaxPlaceholders = 1
	_, err = graph.Load(context.Background(), rows)
	require.ErrorContains(t, err, "exceeding dialect limit")
	require.EqualValues(t, 3, calls.Load(), "over-budget child SQL must not execute")
	dialect.MaxPlaceholders = 3
	cancelled, cancel := context.WithCancel(callerContext)
	defer cancel()
	cancelRead = cancel
	_, err = graph.Load(cancelled, rows)
	require.ErrorIs(t, err, context.Canceled)
	// Compiling a source child with an OnFetch security/output contract is fine,
	// but projection label expansion rejects it before executing a lookup.
	hookType := reflect.TypeFor[[]*projectionHookParent]()
	hookPlan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[projectionInput](), OutputType: hookType, DirectViewType: hookType})
	require.NoError(t, err)
	hookExecution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[projectionInput](), OutputType: hookType, Plan: hookPlan, SQL: &dsql.SQLComponent{DB: connection}})
	require.NoError(t, err)
	hookPrepared, err := hookExecution.PrepareQuery(ctx, &projectionInput{Tenant: "a"}, nil, func(name string) (any, bool, error) { return "a", name == "tenant", nil })
	require.NoError(t, err)
	before := calls.Load()
	_, err = hookPrepared.Projection.(dexec.ProjectionGraphReader).PrepareProjectionGraph(ctx, request)
	require.ErrorContains(t, err, "hook-free child")
	require.Equal(t, before, calls.Load())
}

type projectionNullableLabel struct {
	Tenant string `sqlx:"tenant"`
	ID     int    `sqlx:"id"`
	Name   string `sqlx:"name"`
}
type projectionNullableParent struct {
	Tenant *string                  `sqlx:"tenant"`
	ID     *int                     `sqlx:"id"`
	Label  *projectionNullableLabel `sqlx:"-"`
}

func TestProjectionGraphNullableCompositeKeysNeverReadWholeDictionary(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE facts(tenant TEXT,id INTEGER)", "INSERT INTO facts VALUES(NULL,NULL),('a',NULL),(NULL,1)"))
	var lookups atomic.Int32
	connection := db.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" && strings.Contains(call.SQL, "FROM labels") {
			lookups.Add(1)
		}
		return nil
	})
	component := &spec.Component{RootView: &spec.View{Name: "facts", Source: &spec.ViewSource{SQL: "SELECT tenant,id FROM facts"}, Relations: []*spec.Relation{{Name: "Label", Holder: "Label", Cardinality: spec.CardinalityOne, On: []*spec.RelationLink{{ParentColumn: "tenant", ChildColumn: "tenant"}, {ParentColumn: "id", ChildColumn: "id"}}, View: &spec.View{Name: "labels", Source: &spec.ViewSource{SQL: "SELECT tenant,id,name FROM labels WHERE $COLUMN_IN"}}}}}}
	typ := reflect.TypeFor[[]*projectionNullableParent]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: connection}})
	require.NoError(t, err)
	prepared, err := execution.PrepareQuery(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	type projected struct {
		Owner *string `sqlx:"owner"`
		Key   *int    `sqlx:"key"`
	}
	graph, err := prepared.Projection.(dexec.ProjectionGraphReader).PrepareProjectionGraph(ctx, dexec.ProjectionGraphRequest{RowType: reflect.TypeFor[projected](), Relations: []dexec.ProjectionRelation{{Name: "Label", Output: "label", Keys: []dexec.ProjectionKey{{Source: "tenant", Output: "owner"}, {Source: "id", Output: "key"}}}}})
	require.NoError(t, err)
	read := func() reflect.Value {
		rows, e := prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: "SELECT tenant AS owner,id AS key FROM (" + prepared.SQL + ") q", Args: prepared.Args, RowType: graph.RowType()})
		require.NoError(t, e)
		loaded, e := graph.Load(ctx, rows)
		require.NoError(t, e)
		return reflect.ValueOf(loaded)
	}
	// The dictionary deliberately does not exist. All-incomplete composite keys
	// must preserve rows and never fall through to an unbounded child read.
	rows := read()
	require.Equal(t, 3, rows.Len())
	require.Zero(t, lookups.Load())
	for i := 0; i < rows.Len(); i++ {
		require.True(t, rows.Index(i).Elem().FieldByName("Label").IsNil())
	}
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE labels(tenant TEXT,id INTEGER,name TEXT)", "INSERT INTO labels VALUES('a',1,'allowed'),('b',1,'foreign'),('a',2,'unrelated')", "INSERT INTO facts VALUES('a',1)"))
	rows = read()
	require.Equal(t, 4, rows.Len())
	require.EqualValues(t, 1, lookups.Load())
	for i := 0; i < 3; i++ {
		require.True(t, rows.Index(i).Elem().FieldByName("Label").IsNil())
	}
	label := rows.Index(3).Elem().FieldByName("Label").Interface().(*projectionNullableLabel)
	require.Equal(t, "allowed", label.Name)
}
