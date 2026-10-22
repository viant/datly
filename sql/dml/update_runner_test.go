package dml

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/metadata/info"
	xexec "github.com/viant/xdatly/exec"
	xhandler "github.com/viant/xdatly/handler"
)

type updateDrainDriver struct {
	mu                                             sync.Mutex
	prepares, executes, closes, commits, rollbacks int
	args                                           [][]driver.NamedValue
	closeErr, execErr                              error
	closePanic                                     any
	execute                                        func(context.Context) error
	rollbackDone                                   chan struct{}
}

func (d *updateDrainDriver) Connect(context.Context) (driver.Conn, error) { return d, nil }
func (d *updateDrainDriver) Driver() driver.Driver                        { return d }
func (d *updateDrainDriver) Open(string) (driver.Conn, error)             { return d, nil }
func (d *updateDrainDriver) Close() error                                 { return nil }
func (d *updateDrainDriver) Begin() (driver.Tx, error)                    { return &updateDrainTx{d}, nil }
func (d *updateDrainDriver) Prepare(string) (driver.Stmt, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.prepares++
	return &updateDrainStmt{d}, nil
}
func (d *updateDrainDriver) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.prepares != d.closes {
		return nil, errors.New("retained UPDATE crossed non-UPDATE barrier")
	}
	return driver.RowsAffected(1), nil
}

type updateDrainTx struct{ d *updateDrainDriver }

func (t *updateDrainTx) Commit() error {
	t.d.mu.Lock()
	defer t.d.mu.Unlock()
	t.d.commits++
	if t.d.prepares != t.d.closes {
		return errors.New("commit preceded statement cleanup")
	}
	return nil
}
func (t *updateDrainTx) Rollback() error {
	t.d.mu.Lock()
	defer t.d.mu.Unlock()
	t.d.rollbacks++
	close(t.d.rollbackDone)
	return nil
}

type updateDrainStmt struct{ d *updateDrainDriver }

func (s *updateDrainStmt) Close() error {
	s.d.mu.Lock()
	defer s.d.mu.Unlock()
	s.d.closes++
	if s.d.closePanic != nil {
		panic(s.d.closePanic)
	}
	return s.d.closeErr
}
func (s *updateDrainStmt) NumInput() int                             { return -1 }
func (s *updateDrainStmt) Query([]driver.Value) (driver.Rows, error) { return nil, io.EOF }
func (s *updateDrainStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("ExecContext required")
}
func (s *updateDrainStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.d.mu.Lock()
	s.d.executes++
	s.d.args = append(s.d.args, append([]driver.NamedValue(nil), args...))
	fn, err := s.d.execute, s.d.execErr
	s.d.mu.Unlock()
	if fn != nil {
		if e := fn(ctx); e != nil {
			return nil, e
		}
	}
	return driver.RowsAffected(1), err
}

type updateDrainRow struct {
	Name string                      `sqlx:"name"`
	ID   int                         `sqlx:"id,primaryKey"`
	hook func(context.Context) error `sqlx:"-"`
}

func (r *updateDrainRow) OnUpdate(ctx context.Context) error {
	if r.hook != nil {
		return r.hook(ctx)
	}
	return nil
}

func updateDrainFixture(t *testing.T) (*Data, *updateDrainDriver) {
	t.Helper()
	driver := &updateDrainDriver{rollbackDone: make(chan struct{})}
	db := sql.OpenDB(driver)
	t.Cleanup(func() { _ = db.Close() })
	d := NewData(db)
	d.dialect = &info.Dialect{Transactional: true}
	return d, driver
}

func TestUpdateDrainReuseAndBarriers(t *testing.T) {
	for _, tc := range []struct {
		boundary          string
		prepares, updates int
	}{
		{"none", 1, 4}, {"sql", 2, 4}, {"append", 3, 5}, {"type", 3, 5}, {"table", 3, 5},
	} {
		t.Run(tc.boundary, func(t *testing.T) {
			data, state := updateDrainFixture(t)
			ctx := xexec.WithContext(context.Background(), &xexec.Context{})
			queue := func(id int) {
				t.Helper()
				if err := data.Update("records", &updateDrainRow{ID: id, Name: fmt.Sprint(id)}); err != nil {
					t.Fatal(err)
				}
			}
			queue(1)
			queue(2)
			switch tc.boundary {
			case "sql":
				if err := data.Execute("barrier"); err != nil {
					t.Fatal(err)
				}
			case "append":
				if err := data.append(dataOperation{kind: dataOpUpdate, table: "records", data: &updateDrainRow{ID: 3, Name: "barrier"}, appendBarrier: true}); err != nil {
					t.Fatal(err)
				}
			case "type":
				type alternate struct {
					Name string `sqlx:"name"`
					ID   int    `sqlx:"id,primaryKey"`
				}
				if err := data.Update("records", &alternate{ID: 3, Name: "type"}); err != nil {
					t.Fatal(err)
				}
			case "table":
				if err := data.Update("other", &updateDrainRow{ID: 3, Name: "table"}); err != nil {
					t.Fatal(err)
				}
			}
			queue(4)
			queue(5)
			if err := data.Flush(ctx, ""); err != nil {
				t.Fatal(err)
			}
			if state.prepares != tc.prepares || state.closes != tc.prepares || state.executes != tc.updates || state.commits != 1 {
				t.Fatalf("counts: %+v", state)
			}
			if len(xexec.GetContext(ctx).Metrics) != tc.updates {
				t.Fatal("per-update metrics lost")
			}
			if len(data.MutationReport().Results) != tc.updates {
				t.Fatal("per-update mutation reports lost")
			}
			for _, operation := range data.queue {
				if !operation.executed || operation.reserved {
					t.Fatal("journal state lost")
				}
			}
			// A later drain must not inherit the previous callback's statement.
			queue(6)
			if err := data.Flush(ctx, ""); err != nil {
				t.Fatal(err)
			}
			if state.prepares != tc.prepares+1 || state.closes != tc.prepares+1 || state.commits != 2 {
				t.Fatal("reuse escaped drain")
			}
		})
	}
}

func TestUpdateDrainFreshBindingsAndHooks(t *testing.T) {
	data, state := updateDrainFixture(t)
	hooks := 0
	for id := 1; id <= 8; id++ {
		row := &updateDrainRow{ID: id, Name: fmt.Sprint(id), hook: func(context.Context) error { hooks++; return nil }}
		if err := data.UpdateWithCriteria("records", row, &sqlx.Criteria{Expression: "name <> ?", Placeholders: []any{fmt.Sprint(-id)}}, xhandler.WithIfMatch("name", fmt.Sprint(id-1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := data.Flush(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if hooks != 8 || state.prepares != 1 || state.executes != 8 {
		t.Fatalf("hooks=%d state=%+v", hooks, state)
	}
	for i, args := range state.args {
		want := []any{fmt.Sprint(i + 1), int64(i + 1), fmt.Sprint(i), fmt.Sprint(-i - 1)}
		var got []any
		for _, arg := range args {
			got = append(got, arg.Value)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("row %d: %v != %v", i, got, want)
		}
	}
}

func TestUpdateDrainFailuresPreventCommit(t *testing.T) {
	for _, mode := range []string{"close", "close-panic", "execute-close", "hook-panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			data, state := updateDrainFixture(t)
			execution, cleanup := errors.New("execute failure"), errors.New("cleanup failure")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			row := &updateDrainRow{ID: 1, Name: "value"}
			switch mode {
			case "close":
				state.closeErr = cleanup
			case "close-panic":
				state.closePanic = cleanup
			case "execute-close":
				state.closeErr, state.execErr = cleanup, execution
			case "hook-panic":
				row.hook = func(context.Context) error { panic(execution) }
			case "cancel":
				state.execute = func(ctx context.Context) error { cancel(); return ctx.Err() }
			}
			if err := data.Update("records", row); err != nil {
				t.Fatal(err)
			}
			err := data.Flush(ctx, "")
			if mode == "cancel" {
				// database/sql may perform rollback asynchronously after cancellation.
				<-state.rollbackDone
			}
			state.mu.Lock()
			commits, rollbacks, closes := state.commits, state.rollbacks, state.closes
			state.mu.Unlock()
			if err == nil || commits != 0 || rollbacks != 1 || closes != 1 {
				t.Fatalf("err=%v commits=%d rollbacks=%d closes=%d", err, commits, rollbacks, closes)
			}
			switch mode {
			case "close", "execute-close":
				if !errors.Is(err, cleanup) {
					t.Fatal("lost close error", err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatal("lost cancellation", err)
				}
			default:
				var recovered *dexec.PanicError
				if !errors.As(err, &recovered) {
					t.Fatal("panic escaped containment", err)
				}
			}
			if mode == "execute-close" && !errors.Is(err, execution) {
				t.Fatal("lost execution error", err)
			}
			if err := data.Flush(context.Background(), ""); err == nil {
				t.Fatal("failed drain admitted again")
			}
		})
	}
}

func TestUpdateDrainCallerTransactionRemainsPending(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			data, state := updateDrainFixture(t)
			tx, err := data.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			data = NewData(data.db, WithTx(tx))
			data.dialect = &info.Dialect{Transactional: true}
			if err := data.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			if failed {
				state.closeErr = errors.New("close")
			}
			if err := data.Update("records", &updateDrainRow{ID: 1, Name: "value"}); err != nil {
				t.Fatal(err)
			}
			err = data.Complete(context.Background(), nil)
			if (err != nil) != failed || state.commits != 0 || state.rollbacks != 0 || state.closes != 1 {
				t.Fatalf("caller ownership: %v %+v", err, state)
			}
			if data.TransactionOutcome().State != xhandler.TransactionCallerPending {
				t.Fatal("caller outcome changed")
			}
		})
	}
}

func TestUpdateDrainInvocationCleanupPrecedesCompletion(t *testing.T) {
	data, state := updateDrainFixture(t)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := data.Update("records", &updateDrainRow{ID: 1, Name: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := data.PrepareFinalization(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state.prepares != 1 || state.closes != 1 || state.commits != 0 || state.rollbacks != 0 {
		t.Fatal("preparation completed transaction or retained statement")
	}
	if err := data.Update("records", &updateDrainRow{ID: 2, Name: "second"}); err != nil {
		t.Fatal(err)
	}
	if err := data.Complete(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if state.prepares != 2 || state.closes != 2 || state.commits != 1 {
		t.Fatal("completion reused earlier drain")
	}
}

func TestUpdateDrainGuardFailureClosesPreparedStatement(t *testing.T) {
	data, state := updateDrainFixture(t)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("late guard failure")
	changed := false
	if err := data.RegisterExecutionGuard(func(context.Context) error {
		if changed {
			return failure
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := data.Update("records", &updateDrainRow{ID: 1, Name: "first", hook: func(context.Context) error { changed = true; return nil }}); err != nil {
		t.Fatal(err)
	}
	if err := data.PrepareFinalization(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state.closes != 1 || state.executes != 1 {
		t.Fatal("preparation did not release statement")
	}
	if err := data.Complete(context.Background(), nil); !errors.Is(err, failure) {
		t.Fatal("guard lost", err)
	}
	if state.commits != 0 || state.rollbacks != 1 {
		t.Fatal("guard failure committed")
	}
}

func TestUpdateDrainConcurrentInvocationsAreIsolated(t *testing.T) {
	started, release := make(chan int, 2), make(chan struct{})
	done := make(chan error, 2)
	states := make([]*updateDrainDriver, 2)
	for request := 0; request < 2; request++ {
		data, state := updateDrainFixture(t)
		states[request] = state
		var first sync.Once
		state.execute = func(ctx context.Context) error {
			first.Do(func() { started <- request; <-release })
			return ctx.Err()
		}
		for id := 1; id <= 2; id++ {
			if err := data.Update("records", &updateDrainRow{ID: id, Name: fmt.Sprint(request)}); err != nil {
				t.Fatal(err)
			}
		}
		go func() { done <- data.Flush(context.Background(), "") }()
	}
	// Both independent requests enter execution before either is released.
	first, second := <-started, <-started
	close(release)
	if first == second {
		t.Fatal("one request supplied both starts")
	}
	for range states {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	for request, state := range states {
		if state.prepares != 1 || state.closes != 1 || state.executes != 2 || state.commits != 1 {
			t.Fatal("statement lifetime crossed invocation")
		}
		for _, args := range state.args {
			if args[0].Value != fmt.Sprint(request) {
				t.Fatal("bindings crossed invocation")
			}
		}
	}
}

func TestUpdateDrainNontransactionalDialectDoesNotRetainStatements(t *testing.T) {
	data, state := updateDrainFixture(t)
	data.dialect = &info.Dialect{Transactional: false}
	for id := 1; id <= 2; id++ {
		if err := data.Update("records", &updateDrainRow{ID: id, Name: "value"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := data.Flush(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if state.prepares != 2 || state.closes != 2 || state.executes != 2 {
		t.Fatal("nontransactional dialect retained a statement")
	}
}

func TestUpdateDrainCaughtFailureRetainsCleanupCause(t *testing.T) {
	data, state := updateDrainFixture(t)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	execution, cleanup := errors.New("execute"), errors.New("close")
	state.execErr, state.closeErr = execution, cleanup
	if err := data.Update("records", &updateDrainRow{ID: 1, Name: "value"}); err != nil {
		t.Fatal(err)
	}
	flushErr := data.PrepareFinalization(context.Background())
	completionErr := data.Complete(context.Background(), nil)
	for _, err := range []error{flushErr, completionErr, data.TransactionOutcome().Error} {
		if !errors.Is(err, execution) || !errors.Is(err, cleanup) {
			t.Fatalf("lost retained cause: %v", err)
		}
	}
	if state.commits != 0 || state.rollbacks != 1 || state.closes != 1 {
		t.Fatal("failed flush completed incorrectly")
	}
}

// Both paths use the same SQLX pin, transaction, row mapping and UPDATE code.
// Only the optional runner differs; this is not an end-to-end MDP benchmark.
func BenchmarkUpdateStatementReuse(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		b.Run(fmt.Sprint(reuse), func(b *testing.B) {
			h := sqlite.New(b, sqlite.WithDSN(":memory:"))
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE records (id INTEGER PRIMARY KEY, name TEXT)", "INSERT INTO records VALUES (1,'row')"); err != nil {
				b.Fatal(err)
			}
			data := NewData(h.DB)
			service, err := data.updater(ctx, h.DB, "records")
			if err != nil {
				b.Fatal(err)
			}
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer tx.Rollback()
			row := &updateDrainRow{ID: 1, Name: "next"}
			run := func(runner *update.StatementRunner) error {
				if _, err := service.Exec(ctx, row, tx); err != nil {
					return err
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var err error
					if runner == nil {
						_, err = service.Exec(ctx, row, tx)
					} else {
						_, err = runner.Exec(ctx, service, row, tx)
					}
					if err != nil {
						return err
					}
				}
				b.StopTimer()
				return nil
			}
			if reuse {
				err = update.WithStatementRunner(tx, run)
			} else {
				err = run(nil)
			}
			if err != nil {
				b.Fatal(err)
			}
		})
	}
}
