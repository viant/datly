package reader_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/metadata/database"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
	xexec "github.com/viant/xdatly/exec"
)

// SQLite executes the hint as a comment. A distinct driver identity supplies
// MySQL metadata/version solely to exercise Datly's real dialect boundary.
// These tests do not claim to verify a MySQL optimizer plan.
type executionHintDriver struct{}

func (executionHintDriver) Open(name string) (driver.Conn, error) {
	return (&sqlite3.SQLiteDriver{ConnectHook: func(c *sqlite3.SQLiteConn) error {
		if err := c.RegisterFunc("concat", func(parts ...string) string { return strings.Join(parts, "") }, true); err != nil {
			return err
		}
		return c.RegisterFunc("version", func() string { return "8.0.36" }, true)
	}}).Open(name)
}

var executionHintDriverOnce sync.Once

func hintDB(t *testing.T) *sql.DB {
	t.Helper()
	executionHintDriverOnce.Do(func() {
		sql.Register("datly_execution_hint_mysql", executionHintDriver{})
	})
	typ := reflect.TypeFor[executionHintDriver]()
	require.NoError(t, registry.RegisterDriver(database.Product{Name: "MySQL", Major: 8}, typ.PkgPath(), typ.Name()))
	db, err := sql.Open("datly_execution_hint_mysql", filepath.Join(t.TempDir(), "hint.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec("CREATE TABLE items(id INTEGER, name TEXT, parent_id INTEGER DEFAULT 1); INSERT INTO items(id,name) VALUES(1,'one'),(2,'two')")
	require.NoError(t, err)
	return db
}

func executionHintOption() *spec.ExecutionOptions {
	value := false
	return &spec.ExecutionOptions{MySQL: &spec.MySQLExecutionOptions{ConditionFanoutFilter: &value}}
}

type executionHintRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}

func TestHintedRootExactLazyReplayAndWarmupRejection(t *testing.T) {
	db := hintDB(t)
	ctx := context.Background()
	typ := reflect.TypeFor[[]*executionHintRow]()
	component := &spec.Component{RootView: &spec.View{Name: "items", ExecutionOptions: executionHintOption(), Source: &spec.ViewSource{SQL: "SELECT id,name FROM items ORDER BY id"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	view := plan.Root.View
	service, err := (cacheconfig.Config{Identity: "hinted-items", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	makeExecution := func() (*reader.Execution, error) {
		return reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}, ReadCaches: map[*data.View]cache.Cache{view: service}})
	}
	// Leave an old unhinted native warmup entry in this same cache service.
	view.Spec.ExecutionOptions = nil
	view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{}}
	plain, err := makeExecution()
	require.NoError(t, err)
	_, err = plain.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}})
	require.NoError(t, err)
	view.Cache = nil
	view.Spec.ExecutionOptions = executionHintOption()
	_, err = db.Exec("UPDATE items SET name='updated' WHERE id=1")
	require.NoError(t, err)
	execution, err := makeExecution()
	require.NoError(t, err)
	read := func() (any, error) {
		readCtx := xexec.WithContext(ctx, xexec.New())
		value, err := execution.Read(readCtx, &struct{}{}, nil, nil)
		if err == nil {
			metrics := xexec.GetContext(readCtx).Metrics
			require.Contains(t, metrics[0].Executions[0].SQL, "SELECT /*+ SET_VAR(optimizer_switch='condition_fanout_filter=off') */")
		}
		return value, err
	}
	first, err := read()
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Equal(t, "updated", first.([]*executionHintRow)[0].Name, "must not replay the old unhinted warmup")
	require.Equal(t, "SELECT id,name FROM items ORDER BY id", view.Spec.Source.SQL)
	_, err = execution.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}, Request: dexec.ReaderWarmupRequest{Settings: &spec.CacheWarmupSettings{}}})
	require.ErrorContains(t, err, "warmup is unsupported")
	view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{}}
	_, err = makeExecution()
	require.ErrorContains(t, err, "warmup is unsupported")
	view.Cache = nil
	_, err = db.Exec("DROP TABLE items")
	require.NoError(t, err)
	second, err := read()
	require.NoError(t, err)
	require.Equal(t, first, second, "exact hinted lazy entry must replay without SQL")
	var concurrent sync.WaitGroup
	failures := make(chan error, 16)
	for range 16 {
		concurrent.Add(1)
		go func() {
			defer concurrent.Done()
			value, err := execution.Read(ctx, &struct{}{}, nil, nil)
			if err != nil {
				failures <- err
				return
			}
			if !reflect.DeepEqual(first, value) {
				failures <- fmt.Errorf("concurrent cache replay differs")
			}
		}()
	}
	concurrent.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.False(t, *view.Spec.ExecutionOptions.MySQL.ConditionFanoutFilter)
	// Removing the option sees only the older unhinted entry, not hinted data.
	view.Spec.ExecutionOptions = nil
	old, err := execution.Read(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "one", old.([]*executionHintRow)[0].Name)
}

type executionHintChild struct {
	ID       int    `sqlx:"id"`
	ParentID int    `sqlx:"parent_id"`
	Name     string `sqlx:"name"`
}

type executionHintParent struct {
	ID    int                   `sqlx:"id"`
	Items []*executionHintChild `view:"items,mysqlConditionFanoutFilter=false" on:"ID:id=ParentID:parent_id" sql:"SELECT id,parent_id,name FROM items WHERE $COLUMN_IN"`
}

func TestHintedRelationExactLazyReplay(t *testing.T) {
	ctx := context.Background()
	db := hintDB(t)
	_, err := db.Exec("CREATE TABLE parents(id INTEGER); INSERT INTO parents VALUES(1)")
	require.NoError(t, err)
	component := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"}}}
	typ := reflect.TypeFor[[]*executionHintParent]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	child := plan.Root.Relations[0].Target.View
	require.NotNil(t, child.Spec.ExecutionOptions)
	service, err := (cacheconfig.Config{Identity: "hinted-relation", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}, ReadCaches: map[*data.View]cache.Cache{child: service}})
	require.NoError(t, err)
	read := func() any {
		readCtx := xexec.WithContext(ctx, xexec.New())
		value, err := e.Read(readCtx, &struct{}{}, nil, nil)
		require.NoError(t, err)
		require.Len(t, value.([]*executionHintParent)[0].Items, 2)
		for _, metric := range xexec.GetContext(readCtx).Metrics {
			for _, execution := range metric.Executions {
				if metric.View == "items" {
					require.Contains(t, execution.SQL, "SELECT /*+ SET_VAR")
				} else {
					require.NotContains(t, execution.SQL, "SET_VAR")
				}
			}
		}
		return value
	}
	first := read()
	_, err = db.Exec("DROP TABLE items")
	require.NoError(t, err)
	require.Equal(t, first, read())
}

func TestInactiveHintKeepsSQLiteWarmup(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE items(id INTEGER,name TEXT)", "INSERT INTO items VALUES(1,'one')"))
	typ := reflect.TypeFor[[]*executionHintRow]()
	component := &spec.Component{RootView: &spec.View{Name: "items", ExecutionOptions: executionHintOption(), Source: &spec.ViewSource{SQL: "SELECT id,name FROM items"}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	view := plan.Root.View
	view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{}}
	service, err := (cacheconfig.Config{Identity: "inactive", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
	require.NoError(t, err)
	e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{view: service}})
	require.NoError(t, err)
	_, err = e.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}})
	require.NoError(t, err)
	_, err = e.Read(ctx, &struct{}{}, nil, nil)
	require.NoError(t, err)
}

type executionHintCount struct {
	Total int `sqlx:"total"`
}
type executionHintOutput struct {
	Data  []*executionHintRow
	Count *executionHintCount `parameter:",kind=output,in=summary" view:"count" sql:"SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent"`
}

func TestHintDoesNotEnterNonWindowCount(t *testing.T) {
	for _, countHint := range []bool{false, true} {
		t.Run(map[bool]string{false: "independent unhinted count", true: "independently hinted count"}[countHint], func(t *testing.T) {
			db := hintDB(t)
			component := &spec.Component{RootView: &spec.View{Name: "items", ExecutionOptions: executionHintOption(), Source: &spec.ViewSource{SQL: "SELECT id,name FROM items"}}}
			component.RootView.Relations = []*spec.Relation{{Name: "count", Holder: "Count", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "count", Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent"}}}}
			typ := reflect.TypeFor[executionHintOutput]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewField: "Data"})
			require.NoError(t, err)
			if countHint {
				plan.Root.Relations[0].Target.View.Spec.ExecutionOptions = executionHintOption()
			}
			e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}})
			require.NoError(t, err)
			ctx := xexec.WithContext(context.Background(), xexec.New())
			value, err := e.Read(ctx, &struct{}{}, nil, nil)
			require.NoError(t, err)
			require.Equal(t, 2, value.(*executionHintOutput).Count.Total)
			for _, metric := range xexec.GetContext(ctx).Metrics {
				for _, execution := range metric.Executions {
					if metric.View == "count" {
						require.Equal(t, map[bool]int{false: 0, true: 1}[countHint], strings.Count(execution.SQL, "SET_VAR"))
						if countHint {
							require.Contains(t, execution.SQL, "SELECT /*+ SET_VAR")
						}
					}
				}
			}
		})
	}
}

type hintWarmupCounter struct {
	cache.Cache
	calls int
}

func (c *hintWarmupCounter) IndexBy(context.Context, *sql.DB, string, string, []interface{}, ...interface{}) (int, error) {
	c.calls++
	return 0, nil
}

func TestHintedSummaryRejectsIndexMetaBeforeAnyPopulation(t *testing.T) {
	ctx := context.Background()
	db := hintDB(t)
	component := &spec.Component{RootView: &spec.View{Name: "items", Source: &spec.ViewSource{SQL: "SELECT id,name FROM items"}}}
	component.RootView.Relations = []*spec.Relation{{Name: "count", Holder: "Count", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "count", ExecutionOptions: executionHintOption(), Source: &spec.ViewSource{SQL: "SELECT COUNT(*) AS total FROM ($View.NonWindowSQL) parent"}}}}
	typ := reflect.TypeFor[executionHintOutput]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewField: "Data"})
	require.NoError(t, err)
	root, summary := plan.Root.View, plan.Root.Relations[0].Target.View
	rootCache, summaryCache := &hintWarmupCounter{}, &hintWarmupCounter{}
	config := reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}, ReadCaches: map[*data.View]cache.Cache{root: rootCache, summary: summaryCache}}
	e, err := reader.NewExecution(config)
	require.NoError(t, err)
	_, err = e.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}, Request: dexec.ReaderWarmupRequest{Settings: &spec.CacheWarmupSettings{IndexMeta: true}}})
	require.ErrorContains(t, err, "warmup is unsupported")
	require.Zero(t, rootCache.calls)
	require.Zero(t, summaryCache.calls)
	root.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{IndexMeta: true}}
	_, err = reader.NewExecution(config)
	require.ErrorContains(t, err, "warmup is unsupported")
}

func TestHintedPreparedProjectionKeepsSourceUnhinted(t *testing.T) {
	db := hintDB(t)
	component := &spec.Component{RootView: &spec.View{Name: "items", ExecutionOptions: executionHintOption(), Source: &spec.ViewSource{SQL: "SELECT id,name FROM items"}}}
	typ := reflect.TypeFor[[]*executionHintRow]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}})
	require.NoError(t, err)
	prepared, err := e.PrepareQuery(context.Background(), &struct{}{}, nil, nil)
	require.NoError(t, err)
	require.NotContains(t, prepared.SQL, "SET_VAR")
	ctx := xexec.WithContext(context.Background(), xexec.New())
	value, err := prepared.Projection.ReadProjection(ctx, dexec.ProjectionRequest{SQL: "SELECT id,name FROM (" + prepared.SQL + ") parent ORDER BY id DESC", Args: prepared.Args, RowType: reflect.TypeFor[executionHintRow]()})
	require.NoError(t, err)
	require.Equal(t, 2, value.([]*executionHintRow)[0].ID)
	metrics := xexec.GetContext(ctx).Metrics
	require.Len(t, metrics, 1)
	require.Equal(t, 1, strings.Count(metrics[0].Executions[0].SQL, "SET_VAR"))
	require.Contains(t, metrics[0].Executions[0].SQL, "SELECT /*+ SET_VAR")
	require.NotContains(t, prepared.SQL, "SET_VAR")
}
