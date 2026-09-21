package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type staticDataSource struct {
	data  xhandler.Data
	err   error
	opens int
}

type failingOutputFinalizer struct{ err error }

func (f *failingOutputFinalizer) Finalize(context.Context, error) error { return f.err }

type operationFailure struct{ message string }

func (e *operationFailure) Error() string { return e.message }

func (s *staticDataSource) Open(context.Context) (xhandler.Data, error) {
	s.opens++
	return s.data, s.err
}

func TestEngineDataScopeDoesNotLoseOpenError(t *testing.T) {
	type input struct{}
	expected := errors.New("open data scope")
	source := &staticDataSource{err: expected}
	_, err := New().Execute(context.Background(), Request{
		Input:      testRouteInput(t, reflect.TypeOf(input{})),
		DataSource: source,
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			_, _, _ = invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			return nil, nil
		}),
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected data source error, got %v", err)
	}
}

func TestEngineDataScopeIsLazyAndSharedByCustomHandlerCapabilities(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	type user struct {
		ID   int    `sqlx:"id,primaryKey"`
		Name string `sqlx:"name"`
	}
	source := &staticDataSource{data: sqldml.NewData(h.DB)}
	_, err := New().Execute(ctx, Request{
		Input:      testRouteInput(t, reflect.TypeOf(input{})),
		DataSource: source,
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			resolvedData, found, err := invocation.Binder.Lookup(ctx, xhandler.DataKey)
			if err != nil || !found {
				return nil, errors.New("data capability missing")
			}
			resolvedDML, found, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if err != nil || !found {
				return nil, errors.New("DML capability missing")
			}
			resolvedSequencer, found, err := invocation.Binder.Lookup(ctx, xhandler.SequencerKey)
			if err != nil || !found {
				return nil, errors.New("sequencer capability missing")
			}
			resolvedFlusher, found, err := invocation.Binder.Lookup(ctx, xhandler.FlusherKey)
			if err != nil || !found {
				return nil, errors.New("flusher capability missing")
			}
			data, ok := resolvedDML.(xhandler.DML)
			if !ok {
				return nil, errors.New("DML capability has wrong type")
			}
			if _, exposed := resolvedDML.(xhandler.Data); exposed {
				t.Fatal("DML capability must remain focused")
			}
			if _, ok := resolvedData.(xhandler.Data); !ok {
				return nil, errors.New("data capability has wrong type")
			}
			if _, ok := resolvedSequencer.(xhandler.Sequencer); !ok {
				return nil, errors.New("sequencer capability has wrong type")
			}
			if _, exposed := resolvedSequencer.(xhandler.Data); exposed {
				t.Fatal("sequencer capability must remain focused")
			}
			if _, ok := resolvedFlusher.(xhandler.Flusher); !ok {
				return nil, errors.New("flusher capability has wrong type")
			}
			if _, exposed := resolvedFlusher.(xhandler.Data); exposed {
				t.Fatal("flusher capability must remain focused")
			}
			return nil, data.Insert("users", &user{ID: 1, Name: "Ada"})
		}),
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if source.opens != 1 {
		t.Fatalf("expected one lazy data scope, got %d", source.opens)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE name = 'Ada'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("expected engine-flushed insert, count=%d err=%v", count, err)
	}

	unused := &staticDataSource{data: sqldml.NewData(h.DB)}
	if _, err := New().Execute(ctx, Request{
		Input:      testRouteInput(t, reflect.TypeOf(input{})),
		DataSource: unused,
		Handler:    rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { return nil, nil }),
	}); err != nil {
		t.Fatalf("execute without data lookup failed: %v", err)
	}
	if unused.opens != 0 {
		t.Fatalf("unused data source must remain lazy, opened %d times", unused.opens)
	}
}

func TestEngineDiscardsBufferedWritesWhenHandlerFailsBeforeFlush(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	expected := errors.New("handler failed")
	data := sqldml.NewData(h.DB)
	_, err := New().Execute(ctx, Request{
		Input:      testRouteInput(t, reflect.TypeOf(input{})),
		DataSource: &staticDataSource{data: data},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			resolved, _, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if err := resolved.(xhandler.DML).Execute(`INSERT INTO audit(id) VALUES (1)`); err != nil {
				return nil, err
			}
			return nil, expected
		}),
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected handler error, got %v", err)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expected buffered write to remain unexecuted, count=%d err=%v", count, err)
	}
}

func TestEngineTransactionSQLImmediateAndBufferedCommitTogetherSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	connectors := &dsql.SQLComponent{}
	if err := connectors.RegisterConnector("ci_ads", h.DB); err != nil {
		t.Fatal(err)
	}
	type input struct{}
	_, err := New().Execute(ctx, Request{
		Input:        testRouteInput(t, reflect.TypeOf(input{})),
		DataSource:   sqldml.Source{DB: h.DB},
		Capabilities: rhandler.InvocationCapabilities{Connector: connectors},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			providerValue, found, lookupErr := invocation.Binder.Lookup(ctx, rhandler.TransactionSQLCapabilityKey)
			if lookupErr != nil || !found {
				return nil, fmt.Errorf("transaction SQL lookup: found=%v err=%w", found, lookupErr)
			}
			provider := providerValue.(rhandler.TransactionSQLProvider)
			txSQL, err := provider.Connector(ctx, "ci_ads")
			if err != nil {
				return nil, err
			}
			if _, exposed := txSQL.(interface{ Commit() error }); exposed {
				t.Fatal("transaction SQL must not expose Commit")
			}
			if _, exposed := txSQL.(interface{ Rollback() error }); exposed {
				t.Fatal("transaction SQL must not expose Rollback")
			}
			result, err := txSQL.ExecContext(ctx, `INSERT INTO audit(name) VALUES (?)`, "immediate")
			if err != nil {
				return nil, err
			}
			if affected, err := result.RowsAffected(); err != nil || affected != 1 {
				return nil, fmt.Errorf("RowsAffected=%d err=%v", affected, err)
			}
			if id, err := result.LastInsertId(); err != nil || id == 0 {
				return nil, fmt.Errorf("LastInsertId=%d err=%v", id, err)
			}
			dmlValue, found, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if lookupErr != nil || !found {
				return nil, fmt.Errorf("DML lookup: found=%v err=%w", found, lookupErr)
			}
			return nil, dmlValue.(xhandler.DML).Execute(`INSERT INTO audit(id,name) VALUES (?,?)`, 20, "buffered")
		}),
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("committed rows=%d err=%v", count, err)
	}
}

func TestEngineTransactionSQLRollbackIncludesImmediateAndFlushedWritesSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	connectors := &dsql.SQLComponent{}
	if err := connectors.RegisterConnector("ci_ads", h.DB); err != nil {
		t.Fatal(err)
	}
	expected := errors.New("handler failed")
	type input struct{}
	_, err := New().Execute(ctx, Request{
		Input:        testRouteInput(t, reflect.TypeOf(input{})),
		DataSource:   sqldml.Source{DB: h.DB},
		Capabilities: rhandler.InvocationCapabilities{Connector: connectors},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			providerValue, _, err := invocation.Binder.Lookup(ctx, rhandler.TransactionSQLCapabilityKey)
			if err != nil {
				return nil, err
			}
			txSQL, err := providerValue.(rhandler.TransactionSQLProvider).Connector(ctx, "ci_ads")
			if err != nil {
				return nil, err
			}
			if _, err = txSQL.ExecContext(ctx, `INSERT INTO audit(id,name) VALUES (?,?)`, 1, "immediate"); err != nil {
				return nil, err
			}
			dmlValue, _, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if err != nil {
				return nil, err
			}
			if err = dmlValue.(xhandler.DML).Execute(`INSERT INTO audit(id,name) VALUES (?,?)`, 2, "buffered"); err != nil {
				return nil, err
			}
			flusherValue, _, err := invocation.Binder.Lookup(ctx, xhandler.FlusherKey)
			if err != nil {
				return nil, err
			}
			if err = flusherValue.(xhandler.Flusher).Flush(ctx, ""); err != nil {
				return nil, err
			}
			return nil, expected
		}),
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected handler error, got %v", err)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rolled back rows=%d err=%v", count, err)
	}
}

func TestEngineTransactionSQLNeverImplicitlyFlushesBufferedWritesSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	connectors := &dsql.SQLComponent{}
	if err := connectors.RegisterConnector("ci_ads", h.DB); err != nil {
		t.Fatal(err)
	}
	type input struct{}
	_, err := New().Execute(ctx, Request{
		Input:        testRouteInput(t, reflect.TypeOf(input{})),
		DataSource:   sqldml.Source{DB: h.DB},
		Capabilities: rhandler.InvocationCapabilities{Connector: connectors},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			dmlValue, _, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if err != nil {
				return nil, err
			}
			if err = dmlValue.(xhandler.DML).Execute(`INSERT INTO audit(id) VALUES (1)`); err != nil {
				return nil, err
			}
			providerValue, _, err := invocation.Binder.Lookup(ctx, rhandler.TransactionSQLCapabilityKey)
			if err != nil {
				return nil, err
			}
			txSQL, err := providerValue.(rhandler.TransactionSQLProvider).Connector(ctx, "ci_ads")
			if err != nil {
				return nil, err
			}
			var count int
			if err = txSQL.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil {
				return nil, err
			}
			if count != 0 {
				return nil, fmt.Errorf("immediate SQL implicitly flushed buffered writes")
			}
			flusherValue, _, err := invocation.Binder.Lookup(ctx, xhandler.FlusherKey)
			if err != nil {
				return nil, err
			}
			if err = flusherValue.(xhandler.Flusher).Flush(ctx, ""); err != nil {
				return nil, err
			}
			if err = txSQL.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil {
				return nil, err
			}
			if count != 1 {
				return nil, fmt.Errorf("explicit flush not visible, count=%d", count)
			}
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
}

func TestEngineTransactionSQLSeparatesConnectorUnitsSQLite(t *testing.T) {
	first, second := testharness.NewSQLiteHarness(t), testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	for _, h := range []*testharness.Harness{first, second} {
		if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY)`); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
	}
	connectors := &dsql.SQLComponent{}
	if err := connectors.RegisterConnector("first", first.DB); err != nil {
		t.Fatal(err)
	}
	if err := connectors.RegisterConnector("second", second.DB); err != nil {
		t.Fatal(err)
	}
	type input struct{}
	_, err := New().Execute(ctx, Request{
		Input:        testRouteInput(t, reflect.TypeOf(input{})),
		Capabilities: rhandler.InvocationCapabilities{Connector: connectors},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			providerValue, _, err := invocation.Binder.Lookup(ctx, rhandler.TransactionSQLCapabilityKey)
			if err != nil {
				return nil, err
			}
			provider := providerValue.(rhandler.TransactionSQLProvider)
			firstSQL, err := provider.Connector(ctx, "first")
			if err != nil {
				return nil, err
			}
			secondSQL, err := provider.Connector(ctx, "second")
			if err != nil {
				return nil, err
			}
			if _, err = firstSQL.ExecContext(ctx, `INSERT INTO audit(id) VALUES (1)`); err != nil {
				return nil, err
			}
			_, err = secondSQL.ExecContext(ctx, `INSERT INTO audit(id) VALUES (2)`)
			return nil, err
		}),
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	var firstCount, secondCount int
	if err := first.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit WHERE id=1`).Scan(&firstCount); err != nil || firstCount != 1 {
		t.Fatalf("first rows=%d err=%v", firstCount, err)
	}
	if err := second.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit WHERE id=2`).Scan(&secondCount); err != nil || secondCount != 1 {
		t.Fatalf("second rows=%d err=%v", secondCount, err)
	}
}

func TestEngineSuppressesOutputAndSuccessFinalizerWhenRootFlushFails(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	type input struct{}
	output := &finalizingOutput{}

	actual, err := New().Execute(context.Background(), Request{
		Input:      testRouteInput(t, reflect.TypeOf(input{})),
		DataSource: &staticDataSource{data: sqldml.NewData(h.DB)},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			resolved, found, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if lookupErr != nil || !found {
				return nil, fmt.Errorf("DML lookup: found=%v: %w", found, lookupErr)
			}
			if err := resolved.(xhandler.DML).Execute(`INSERT INTO missing_table(id) VALUES (1)`); err != nil {
				return nil, err
			}
			return output, nil
		}),
	})
	if err == nil {
		t.Fatal("expected root flush failure")
	}
	if actual != nil {
		t.Fatalf("completion-only failure must suppress output, got %#v", actual)
	}
	if output.called {
		t.Fatal("success finalizer must not run after root flush failure")
	}
}

func TestEnginePreservesConcreteOperationErrorAndOutputAfterRollback(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{}
	expected := &operationFailure{message: "operation failed"}
	output := &finalizingOutput{}

	actual, err := New().Execute(ctx, Request{
		Input:      testRouteInput(t, reflect.TypeOf(input{})),
		DataSource: &staticDataSource{data: sqldml.NewData(h.DB)},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			dmlValue, found, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if lookupErr != nil || !found {
				return nil, fmt.Errorf("DML lookup: found=%v: %w", found, lookupErr)
			}
			flusherValue, found, lookupErr := invocation.Binder.Lookup(ctx, xhandler.FlusherKey)
			if lookupErr != nil || !found {
				return nil, fmt.Errorf("flusher lookup: found=%v: %w", found, lookupErr)
			}
			if queueErr := dmlValue.(xhandler.DML).Execute(`INSERT INTO audit(id) VALUES (1)`); queueErr != nil {
				return nil, queueErr
			}
			if flushErr := flusherValue.(xhandler.Flusher).Flush(ctx, "audit"); flushErr != nil {
				return nil, flushErr
			}
			return output, expected
		}),
	})
	if err != expected {
		t.Fatalf("expected exact concrete operation error, got %T %v", err, err)
	}
	if actual != output {
		t.Fatalf("structured output must survive operation failure, got %#v", actual)
	}
	if output.called {
		t.Fatal("success finalizer must not run after operation failure")
	}
	var count int
	if queryErr := h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); queryErr != nil || count != 0 {
		t.Fatalf("root completion must roll back explicitly flushed work, count=%d err=%v", count, queryErr)
	}
}

func TestEngineDataScopeLeavesExternalTransactionOpen(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin external transaction failed: %v", err)
	}
	type input struct{}
	type audit struct {
		ID int `sqlx:"id,primaryKey"`
	}
	source := &staticDataSource{data: sqldml.NewData(h.DB, sqldml.WithTx(tx))}
	_, err = New().Execute(ctx, Request{
		Input:      testRouteInput(t, reflect.TypeOf(input{})),
		DataSource: source,
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			resolved, _, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if _, exposed := resolved.(xhandler.Data); exposed {
				t.Fatal("DML key must not expose the complete data capability")
			}
			return nil, resolved.(xhandler.DML).Insert("audit", &audit{ID: 1})
		}),
	})
	if err != nil {
		t.Fatalf("execute with external transaction failed: %v", err)
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("expected uncommitted row in external transaction, count=%d err=%v", count, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO audit(id) VALUES (2)`); err != nil {
		t.Fatalf("engine must leave external transaction open: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("root rollback failed: %v", err)
	}
	if err := h.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expected root rollback to own transaction, count=%d err=%v", count, err)
	}
}

func TestEngineRollsBackLocalTransactionWhenOutputFinalizerFails(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	type input struct{}
	expected := errors.New("output finalizer failed")
	_, err := New().Execute(ctx, Request{
		Input:      testRouteInput(t, reflect.TypeOf(input{})),
		DataSource: &staticDataSource{data: sqldml.NewData(h.DB)},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			resolved, _, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if err := resolved.(xhandler.DML).Execute("INSERT INTO audit(id) VALUES (1)"); err != nil {
				return nil, err
			}
			return &failingOutputFinalizer{err: expected}, nil
		}),
	})
	if !errors.Is(err, expected) {
		t.Fatalf("Execute() error=%v", err)
	}
	var count int
	if err = h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestEngineRootRollsBackWhenNestedComponentFailsDuringBinding(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	type parentInput struct{ Child int }
	type childInput struct{}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	binding := bindly.BindingSpec{
		Path: "Child", Location: bindstate.Location{Kind: "component", In: "GET:/child"},
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(parentInput{}), binding)
	if err != nil {
		t.Fatal(err)
	}
	rootData := sqldml.NewData(harness.DB)
	rootSource := &staticDataSource{data: rootData}
	expected := errors.New("child failed")
	componentProvider := handlerprovider.Named("component", func(ctx context.Context, _ reflect.Type, _ string) (any, bool, error) {
		value, nestedErr := New().Execute(ctx, Request{
			Input: testRouteInput(t, reflect.TypeOf(childInput{})), DataSource: rootSource,
			Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				if _, found, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey); lookupErr != nil || !found {
					return nil, errors.New("nested DML capability missing")
				}
				return nil, expected
			}),
		})
		return value, nestedErr == nil, nestedErr
	})
	invoked := false
	_, err = New().Execute(context.Background(), Request{
		Injector: injector, Input: testRouteInputWithPlan(t, reflect.TypeOf(parentInput{}), plan, binding),
		Components: componentProvider, DataSource: rootSource,
		Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
			invoked = true
			return nil, nil
		}),
	})
	if !errors.Is(err, expected) || invoked {
		t.Fatalf("Execute() = invoked:%v error:%v", invoked, err)
	}
	if rootSource.opens != 1 {
		t.Fatalf("data lifecycle root opens:%d", rootSource.opens)
	}
}

func TestEngineComponentBindingSharesGlobalTransaction(t *testing.T) {
	tests := []struct {
		name   string
		commit bool
		want   int
	}{
		{name: "external owner commits", commit: true, want: 1},
		{name: "external owner rolls back", want: 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := harness.ExecStatements(ctx,
				`CREATE TABLE campaign (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`,
				`CREATE TABLE flight (id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES campaign(id))`,
			); err != nil {
				t.Fatal(err)
			}
			conn, err := harness.DB.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			if _, err = conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
				t.Fatal(err)
			}
			globalTx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = globalTx.Rollback() })

			type campaignRow struct {
				ID   int    `sqlx:"id,primaryKey"`
				Name string `sqlx:"name"`
			}
			type flightRow struct {
				ID         int `sqlx:"id,primaryKey"`
				CampaignID int `sqlx:"campaign_id"`
			}
			type childInput struct{}
			type childOutput struct{ CampaignID int }
			type parentInput struct{ Child *childOutput }

			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			binding := bindly.BindingSpec{
				Path: "Child", Location: bindstate.Location{Kind: "component", In: "POST:/global-tx-child"},
			}
			plan, err := injector.CompilePlan(reflect.TypeOf(parentInput{}), binding)
			if err != nil {
				t.Fatal(err)
			}
			childSource := sqldml.Source{DB: harness.DB}
			componentProvider := handlerprovider.Named("component", func(ctx context.Context, _ reflect.Type, _ string) (any, bool, error) {
				value, nestedErr := New().Execute(ctx, Request{
					Input: testRouteInput(t, reflect.TypeOf(childInput{})), DataSource: childSource,
					Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
						resolved, found, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
						if lookupErr != nil || !found {
							return nil, fmt.Errorf("child DML capability: found=%v err=%w", found, lookupErr)
						}
						output := &childOutput{CampaignID: 10}
						if insertErr := resolved.(xhandler.DML).Insert("flight", &flightRow{ID: 20, CampaignID: output.CampaignID}); insertErr != nil {
							return nil, insertErr
						}
						return output, nil
					}),
				})
				return value, nestedErr == nil, nestedErr
			})
			_, err = New().Execute(ctx, Request{
				Injector:   injector,
				Input:      testRouteInputWithPlan(t, reflect.TypeOf(parentInput{}), plan, binding),
				Components: componentProvider,
				DataSource: sqldml.Source{DB: harness.DB, Tx: globalTx},
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					input := invocation.Input.(*parentInput)
					if input.Child == nil || input.Child.CampaignID != 10 {
						return nil, errors.New("bound child output missing")
					}
					resolved, found, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
					if lookupErr != nil || !found {
						return nil, errors.New("parent DML capability missing")
					}
					return nil, resolved.(xhandler.DML).Insert("campaign", &campaignRow{ID: input.Child.CampaignID, Name: "root-owned"})
				}),
			})
			if err != nil {
				t.Fatalf("execute root component: %v", err)
			}
			for _, table := range []string{"campaign", "flight"} {
				var count int
				if err = globalTx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 1 {
					t.Fatalf("global transaction %s count = %d, want 1, err = %v", table, count, err)
				}
			}
			if testCase.commit {
				err = globalTx.Commit()
			} else {
				err = globalTx.Rollback()
			}
			if err != nil {
				t.Fatalf("complete global transaction: %v", err)
			}
			for _, table := range []string{"campaign", "flight"} {
				var count int
				if err = harness.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != testCase.want {
					t.Fatalf("committed %s count = %d, want %d, err = %v", table, count, testCase.want, err)
				}
			}
		})
	}
}

func TestEngineSeparatesDatabaseUnitsAndCompletesThemAtRoot(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		rootFail bool
		want     int
	}{{name: "commit", want: 1}, {name: "rollback", rootFail: true}} {
		t.Run(testCase.name, func(t *testing.T) {
			rootDB := testharness.NewSQLiteHarness(t)
			childDB := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := rootDB.ExecStatements(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			if err := childDB.ExecStatements(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			type parentInput struct{ Child int }
			type childInput struct{}
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			binding := bindly.BindingSpec{Path: "Child", Location: bindstate.Location{Kind: "component", In: "GET:/child"}}
			plan, err := injector.CompilePlan(reflect.TypeOf(parentInput{}), binding)
			if err != nil {
				t.Fatal(err)
			}
			componentProvider := handlerprovider.Named("component", func(ctx context.Context, _ reflect.Type, _ string) (any, bool, error) {
				value, nestedErr := New().Execute(ctx, Request{
					Input: testRouteInput(t, reflect.TypeOf(childInput{})), DataSource: sqldml.Source{DB: childDB.DB},
					Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
						resolved, _, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
						if lookupErr != nil {
							return nil, lookupErr
						}
						if err := resolved.(xhandler.DML).Execute("INSERT INTO audit(id) VALUES (1)"); err != nil {
							return nil, err
						}
						return 1, nil
					}),
				})
				return value, nestedErr == nil, nestedErr
			})
			expected := errors.New("root failed")
			_, err = New().Execute(ctx, Request{
				Injector: injector, Input: testRouteInputWithPlan(t, reflect.TypeOf(parentInput{}), plan, binding),
				Components: componentProvider, DataSource: sqldml.Source{DB: rootDB.DB},
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					resolved, _, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
					if lookupErr != nil {
						return nil, lookupErr
					}
					if err := resolved.(xhandler.DML).Execute("INSERT INTO audit(id) VALUES (1)"); err != nil {
						return nil, err
					}
					if testCase.rootFail {
						return nil, expected
					}
					return nil, nil
				}),
			})
			if testCase.rootFail {
				if !errors.Is(err, expected) {
					t.Fatalf("Execute() error=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			for name, db := range map[string]*sql.DB{"root": rootDB.DB, "child": childDB.DB} {
				var count int
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != testCase.want {
					t.Fatalf("%s count=%d want=%d err=%v", name, count, testCase.want, err)
				}
			}
		})
	}
}

func TestEngineRollsBackPreparedRootUnitWhenChildUnitFlushFails(t *testing.T) {
	rootDB := testharness.NewSQLiteHarness(t)
	childDB := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := rootDB.ExecStatements(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	type parentInput struct{ Child int }
	type childInput struct{}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	binding := bindly.BindingSpec{Path: "Child", Location: bindstate.Location{Kind: "component", In: "GET:/child"}}
	plan, err := injector.CompilePlan(reflect.TypeOf(parentInput{}), binding)
	if err != nil {
		t.Fatal(err)
	}
	componentProvider := handlerprovider.Named("component", func(ctx context.Context, _ reflect.Type, _ string) (any, bool, error) {
		value, nestedErr := New().Execute(ctx, Request{
			Input: testRouteInput(t, reflect.TypeOf(childInput{})), DataSource: sqldml.Source{DB: childDB.DB},
			Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				resolved, _, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
				if lookupErr != nil {
					return nil, lookupErr
				}
				if queueErr := resolved.(xhandler.DML).Execute("INSERT INTO missing_table(id) VALUES (1)"); queueErr != nil {
					return nil, queueErr
				}
				return 1, nil
			}),
		})
		return value, nestedErr == nil, nestedErr
	})
	actual, err := New().Execute(ctx, Request{
		Injector: injector, Input: testRouteInputWithPlan(t, reflect.TypeOf(parentInput{}), plan, binding),
		Components: componentProvider, DataSource: sqldml.Source{DB: rootDB.DB},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			resolved, _, lookupErr := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if queueErr := resolved.(xhandler.DML).Execute("INSERT INTO audit(id) VALUES (1)"); queueErr != nil {
				return nil, queueErr
			}
			return &struct{}{}, nil
		}),
	})
	if err == nil {
		t.Fatal("expected child database flush failure")
	}
	if actual != nil {
		t.Fatalf("completion failure must suppress result, got %#v", actual)
	}
	var count int
	if queryErr := rootDB.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); queryErr != nil || count != 0 {
		t.Fatalf("prepared root unit must roll back, count=%d err=%v", count, queryErr)
	}
}

func TestSecondaryDatabaseSiblingsReceiveSeparateComponentFrames(t *testing.T) {
	rootDB := testharness.NewSQLiteHarness(t)
	childDB := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	root, _ := invocationDataScope(ctx, sqldml.Source{DB: rootDB.DB})
	parentCtx := withDataScope(ctx, root)
	if _, err := root.resolve(parentCtx); err != nil {
		t.Fatal(err)
	}

	firstCtx := PrepareComponent(parentCtx, ComponentImperative, "")
	first, _ := invocationDataScope(firstCtx, sqldml.Source{DB: childDB.DB})
	firstData, err := first.resolve(firstCtx)
	if err != nil {
		t.Fatal(err)
	}
	if queueErr := firstData.Execute("first"); queueErr != nil {
		t.Fatal(queueErr)
	}
	completeDataScope(firstCtx, first, false, nil)

	secondCtx := PrepareComponent(parentCtx, ComponentImperative, "")
	second, _ := invocationDataScope(secondCtx, sqldml.Source{DB: childDB.DB})
	secondData, err := second.resolve(secondCtx)
	if err != nil {
		t.Fatal(err)
	}
	if firstData == secondData {
		t.Fatal("secondary database siblings must not share a component frame")
	}
	if queueErr := secondData.Execute("second"); queueErr != nil {
		t.Fatalf("second sibling append failed after first sealed: %v", queueErr)
	}
	completeDataScope(secondCtx, second, false, nil)

	unitData := root.units[0].data.(*sqldml.Data)
	journal := unitData.ComponentData(sqldml.ComponentImperative, "").(*sqldml.Data)
	if queueErr := journal.Execute("third"); queueErr != nil {
		t.Fatalf("secondary database unit root was sealed: %v", queueErr)
	}
	_ = root.complete(ctx, errors.New("abort"))
}

func TestDataScopeRejectsUnknownNestedDatabaseIdentity(t *testing.T) {
	rootSource := &staticDataSource{data: nil}
	childSource := &staticDataSource{data: nil}
	ctx := context.Background()
	root, _ := invocationDataScope(ctx, rootSource)
	ctx = withDataScope(ctx, root)
	child, _ := invocationDataScope(ctx, childSource)
	if _, err := child.resolve(ctx); !errors.Is(err, ErrUnknownDatabaseIdentity) {
		t.Fatalf("resolve() error=%v", err)
	}
}

func TestDataScopeRejectsConflictingTransactionForSameDatabase(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	txA, err := h.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txA.Rollback()
	txB, err := h.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer txB.Rollback()
	ctx := context.Background()
	root, _ := invocationDataScope(ctx, sqldml.Source{DB: h.DB, Tx: txA})
	ctx = withDataScope(ctx, root)
	child, _ := invocationDataScope(ctx, sqldml.Source{DB: h.DB, Tx: txB})
	if _, err = child.resolve(ctx); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("resolve() error=%v", err)
	}
}
