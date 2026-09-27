package reader_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

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
	xreader "github.com/viant/xdatly/reader"
)

type transactionCacheChild struct {
	ID       int    `sqlx:"id"`
	ParentID int    `sqlx:"parent_id"`
	Name     string `sqlx:"name"`
}

type transactionCacheRow struct {
	ID       int                      `sqlx:"id"`
	Name     string                   `sqlx:"name"`
	Children []*transactionCacheChild `view:"children,connector=main" on:"ID:id=ParentID:parent_id" sql:"SELECT id,parent_id,name FROM children WHERE $COLUMN_IN"`
	External []*transactionCacheChild `view:"external,connector=other" on:"ID:id=ParentID:parent_id" sql:"SELECT id,parent_id,name FROM children WHERE $COLUMN_IN"`
}

type transactionCacheSummary struct {
	Name string `sqlx:"name"`
}
type transactionCacheOutput struct {
	Data    []*transactionCacheRow   `parameter:",kind=output,in=view" view:"parents,connector=main" sql:"SELECT id,name FROM parents ORDER BY id"`
	Summary *transactionCacheSummary `parameter:",kind=output,in=summary" view:"summary,connector=main" sql:"SELECT MAX(name) AS name FROM ($View.parents.NonWindowSQL) source"`
}

// Count native cache access without replacing SQLX's real filesystem cache.
type transactionCacheProbe struct {
	cache.Cache
	reads  atomic.Int64
	writes atomic.Int64
}

func (c *transactionCacheProbe) Get(ctx context.Context, query string, args []any, options ...any) (*cache.Entry, error) {
	c.reads.Add(1)
	return c.Cache.Get(ctx, query, args, options...)
}
func (c *transactionCacheProbe) AddValues(ctx context.Context, entry *cache.Entry, values []any) error {
	c.writes.Add(1)
	return c.Cache.AddValues(ctx, entry, values)
}
func (c *transactionCacheProbe) IndexBy(ctx context.Context, db *sql.DB, column, query string, args []any, options ...any) (int, error) {
	c.writes.Add(1)
	return c.Cache.IndexBy(ctx, db, column, query, args, options...)
}
func (c *transactionCacheProbe) Close(ctx context.Context, entry *cache.Entry) error {
	c.writes.Add(1)
	return c.Cache.Close(ctx, entry)
}

type transactionCacheFixture struct {
	db, other *sqlite.Harness
	execution *reader.Execution
	plan      *reader.Plan
	probes    map[string]*transactionCacheProbe
}

func newTransactionCacheFixture(t *testing.T, cacheRoot bool) *transactionCacheFixture {
	t.Helper()
	ctx := context.Background()
	f := &transactionCacheFixture{db: sqlite.New(t), other: sqlite.New(t), probes: map[string]*transactionCacheProbe{}}
	require.NoError(t, f.db.ExecStatements(ctx, "PRAGMA journal_mode=WAL", "CREATE TABLE parents(id INTEGER,name TEXT)", "INSERT INTO parents VALUES(1,'committed')", "CREATE TABLE children(id INTEGER,parent_id INTEGER,name TEXT)", "INSERT INTO children VALUES(10,1,'committed')"))
	require.NoError(t, f.other.ExecStatements(ctx, "CREATE TABLE children(id INTEGER,parent_id INTEGER,name TEXT)", "INSERT INTO children VALUES(20,1,'remote')"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "TransactionalCache"}, RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id,name FROM parents ORDER BY id"}}}
	component.RootView.Relations = []*spec.Relation{{Name: "summary", Holder: "Summary", Kind: spec.RelationKindDerived, Cardinality: spec.CardinalityOne, View: &spec.View{Name: "summary", Source: &spec.ViewSource{SQL: "SELECT MAX(name) AS name FROM ($View.NonWindowSQL) source", Bindings: &spec.ViewBindings{Connector: "main"}}}}}
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[transactionCacheOutput]()})
	require.NoError(t, err)
	f.plan = plan
	caches := map[*data.View]cache.Cache{}
	var configure func(*data.View)
	configure = func(view *data.View) {
		if cacheRoot || view != plan.Root.View {
			native, err := (cacheconfig.Config{Identity: "transaction/" + view.Spec.Name, Settings: &spec.CacheSettings{Enabled: true, Provider: "afs", Location: t.TempDir(), TTL: "1h"}}).New()
			require.NoError(t, err)
			probe := &transactionCacheProbe{Cache: native}
			f.probes[view.Spec.Name] = probe
			caches[view] = probe
			view.Cache = &data.Cache{Warmup: &spec.CacheWarmupSettings{}}
		}
		for _, relation := range view.Relations {
			configure(relation.Of.View)
		}
	}
	configure(plan.Root.View)
	source := &dsql.SQLComponent{DB: f.db.DB}
	require.NoError(t, source.RegisterConnector("main", f.db.DB))
	require.NoError(t, source.RegisterConnector("other", f.other.DB))
	f.execution, err = reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[transactionCacheOutput](), Plan: plan, SQL: source, ReadCaches: caches})
	require.NoError(t, err)
	return f
}

func (f *transactionCacheFixture) transaction(t *testing.T) (context.Context, *sql.Tx) {
	t.Helper()
	tx, err := f.db.DB.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	ctx := dexec.WithInvocationTransactionLookup(context.Background(), func(_ context.Context, db *sql.DB) (*sql.Tx, error) {
		if db == f.db.DB {
			return tx, nil
		}
		return nil, nil
	})
	return ctx, tx
}

func (f *transactionCacheFixture) read(ctx context.Context) (*transactionCacheOutput, error) {
	value, err := f.execution.Read(ctx, &struct{}{}, nil, nil)
	if err != nil {
		return nil, err
	}
	return value.(*transactionCacheOutput), nil
}

func assertTransactionCacheOutput(t *testing.T, output *transactionCacheOutput, name string) {
	t.Helper()
	require.Len(t, output.Data, 1)
	require.Equal(t, name, output.Data[0].Name)
	require.Len(t, output.Data[0].Children, 1)
	require.Equal(t, name, output.Data[0].Children[0].Name)
	require.Len(t, output.Data[0].External, 1)
	require.Equal(t, "remote", output.Data[0].External[0].Name)
	require.NotNil(t, output.Summary)
	require.Equal(t, name, output.Summary.Name)
}

func TestTransactionalViewsBypassCachesSQLite(t *testing.T) {
	for _, rootCached := range []bool{false, true} {
		for _, prime := range []bool{false, true} {
			t.Run(fmt.Sprintf("root_cached=%v/primed=%v", rootCached, prime), func(t *testing.T) {
				f := newTransactionCacheFixture(t, rootCached)
				ctx := context.Background()
				if prime {
					out, err := f.read(ctx)
					require.NoError(t, err)
					assertTransactionCacheOutput(t, out, "committed")
					// Independent connector retains cache reuse during the MySQL-like transaction.
					require.NoError(t, f.other.ExecStatements(ctx, "DROP TABLE children"))
				}
				txCtx, tx := f.transaction(t)
				_, err := tx.ExecContext(txCtx, "UPDATE parents SET name='uncommitted'; UPDATE children SET name='uncommitted'")
				require.NoError(t, err)
				before := map[string][2]int64{}
				for name, probe := range f.probes {
					before[name] = [2]int64{probe.reads.Load(), probe.writes.Load()}
				}
				out, err := f.read(txCtx)
				require.NoError(t, err)
				assertTransactionCacheOutput(t, out, "uncommitted")
				for name, probe := range f.probes {
					if name == "external" {
						require.Greater(t, probe.reads.Load(), before[name][0])
						continue
					}
					require.Equal(t, before[name], [2]int64{probe.reads.Load(), probe.writes.Load()}, "transaction must never access cache %s", name)
				}
				require.NoError(t, tx.Rollback())
				out, err = f.read(ctx)
				require.NoError(t, err)
				assertTransactionCacheOutput(t, out, "committed")
				require.NoError(t, f.db.ExecStatements(ctx, "DROP TABLE children"))
				out, err = f.read(ctx)
				require.NoError(t, err)
				assertTransactionCacheOutput(t, out, "committed")
			})
		}
	}
}

func TestTransactionalCachePolicyConcurrentSQLite(t *testing.T) {
	f := newTransactionCacheFixture(t, true)
	_, err := f.read(context.Background())
	require.NoError(t, err)
	ctx, tx := f.transaction(t)
	_, err = tx.ExecContext(ctx, "UPDATE parents SET name='uncommitted'; UPDATE children SET name='uncommitted'")
	require.NoError(t, err)
	var workers sync.WaitGroup
	errors := make(chan error, 12)
	start := make(chan struct{})
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func(transactional bool) {
			defer workers.Done()
			<-start
			readCtx, want := context.Background(), "committed"
			if transactional {
				readCtx, want = ctx, "uncommitted"
			}
			out, err := f.read(readCtx)
			if err == nil && (len(out.Data) != 1 || out.Data[0].Name != want || len(out.Data[0].Children) != 1 || out.Data[0].Children[0].Name != want || out.Summary == nil || out.Summary.Name != want) {
				err = fmt.Errorf("transactional=%v received wrong snapshot: %+v", transactional, out)
			}
			errors <- err
		}(i%2 == 0)
	}
	close(start)
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

func TestTransactionalReaderCacheOnlyAndWarmupRejectedSQLite(t *testing.T) {
	f := newTransactionCacheFixture(t, true)
	ctx, _ := f.transaction(t)
	_, err := f.read((dexec.ReaderOptions{CacheOnly: true}).Context(ctx))
	require.ErrorContains(t, err, "cache-only mode")
	count, err := f.execution.Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &struct{}{}})
	require.ErrorContains(t, err, "does not support cache warmup")
	require.Zero(t, count)
	for name, probe := range f.probes {
		require.Zero(t, probe.reads.Load(), name)
		require.Zero(t, probe.writes.Load(), name)
	}
}

func TestTransactionalRootSkipsIndexedWarmupPreparationSQLite(t *testing.T) {
	f := newTransactionCacheFixture(t, true)
	f.plan.Root.View.Cache.Warmup = &spec.CacheWarmupSettings{IndexColumn: "id", IndexParameter: "ID"}
	ctx, _ := f.transaction(t)
	_, err := f.execution.Read(ctx, &struct{}{}, nil, func(string) (any, bool, error) { return nil, false, fmt.Errorf("warmup preparation must not run") })
	require.NoError(t, err)
}

func TestTransactionalCacheRefreshDoesNotPopulateSQLite(t *testing.T) {
	f := newTransactionCacheFixture(t, true)
	ctx, tx := f.transaction(t)
	_, err := tx.ExecContext(ctx, "UPDATE parents SET name='uncommitted'; UPDATE children SET name='uncommitted'")
	require.NoError(t, err)
	out, err := f.read((dexec.ReaderOptions{RefreshCache: true}).Context(ctx))
	require.NoError(t, err)
	assertTransactionCacheOutput(t, out, "uncommitted")
	for name, probe := range f.probes {
		if name == "external" {
			require.Positive(t, probe.reads.Load())
			continue
		}
		require.Zero(t, probe.reads.Load(), name)
		require.Zero(t, probe.writes.Load(), name)
	}
}

type transactionCachePartitioner struct{}

func (transactionCachePartitioner) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return nil, fmt.Errorf("partitioner must not run in a transaction")
}

func TestTransactionalPartitionRejectionPreservedSQLite(t *testing.T) {
	f := newTransactionCacheFixture(t, true)
	f.plan.Root.Partitioner = transactionCachePartitioner{}
	ctx, _ := f.transaction(t)
	_, err := f.read(ctx)
	require.ErrorContains(t, err, "does not support partitioned view parents")
	for _, probe := range f.probes {
		require.Zero(t, probe.reads.Load())
		require.Zero(t, probe.writes.Load())
	}
}
