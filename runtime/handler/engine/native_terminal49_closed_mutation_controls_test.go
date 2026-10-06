package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

type terminal49Row struct {
	ID    int    `sqlx:"id,primaryKey=true"`
	Label string `sqlx:"label"`
}
type terminal49Output struct {
	t                *testing.T
	root             *dataScope
	native, borrowed *dml.Data
	tx               *sql.Tx
	call             func(context.Context, *dml.Data) error
	useBorrowed      bool
	attempt          error
	calls            int
	before, after    []int
}

func (o *terminal49Output) Finalize(ctx context.Context, cause error) error {
	o.calls++
	if cause != nil {
		o.t.Fatalf("intended successful terminal stage not reached: %v", cause)
	}
	if !o.root.completionStarted {
		o.t.Fatal("terminal callback preceded real protected admission close")
	}
	o.before = terminal49IDs(o.t, o.tx)
	target := o.native
	if o.useBorrowed {
		target = o.borrowed
	}
	o.attempt = o.call(ctx, target)
	o.after = terminal49IDs(o.t, o.tx)
	if !errors.Is(o.attempt, dml.ErrMutationAdmissionClosed) {
		o.t.Errorf("DESIRED_NATIVE_TERMINAL_ADMISSION_GAP returned=%v", o.attempt)
	}
	if !reflect.DeepEqual(o.before, o.after) {
		o.t.Errorf("DESIRED_NATIVE_TERMINAL_SQL_GAP rows=%v -> %v", o.before, o.after)
	}
	// Deliberately caught. Root must collect a protected native mutation attempt
	// rather than publish success solely because the public call rejected SQL.
	return nil
}
func terminal49IDs(t *testing.T, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) []int {
	t.Helper()
	rows, e := q.QueryContext(t.Context(), "SELECT id FROM records ORDER BY id")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	var values []int
	for rows.Next() {
		var id int
		if e = rows.Scan(&id); e != nil {
			t.Fatal(e)
		}
		values = append(values, id)
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	return values
}
func TestNativeTerminal49DesiredIgnoredClosedMutation(t *testing.T) {
	calls := []struct {
		name string
		call func(context.Context, *dml.Data) error
	}{
		{"Execute", func(_ context.Context, d *dml.Data) error {
			return d.Execute("INSERT INTO records VALUES(2,'late execute')")
		}},
		{"Insert", func(_ context.Context, d *dml.Data) error {
			return d.Insert("records", &terminal49Row{ID: 2, Label: "late insert"})
		}},
		{"Update", func(_ context.Context, d *dml.Data) error {
			return d.Update("records", &terminal49Row{ID: 99, Label: "late update"})
		}},
		{"Delete", func(_ context.Context, d *dml.Data) error {
			return d.Delete("records", &terminal49Row{ID: 99, Label: "prior"})
		}},
		{"Start", func(ctx context.Context, d *dml.Data) error { return d.Start(ctx) }},
		{"Reserve", func(ctx context.Context, d *dml.Data) error {
			return d.Reserve(ctx, "records", &terminal49Row{ID: 123, Label: "supplied"}, "ID")
		}},
		{"Allocate", func(ctx context.Context, d *dml.Data) error {
			return d.Allocate(ctx, "records", &terminal49Row{Label: "new"}, "ID")
		}},
		{"ExecContext", func(ctx context.Context, d *dml.Data) error {
			_, e := d.ExecContext(ctx, "INSERT INTO records VALUES(2,'late immediate')")
			return e
		}},
		{"QueryContext", func(ctx context.Context, d *dml.Data) error {
			rows, e := d.QueryContext(ctx, "INSERT INTO records VALUES(2,'late query') RETURNING id")
			if rows != nil {
				rows.Close()
			}
			return e
		}},
		{"QueryRowContext", func(ctx context.Context, d *dml.Data) error {
			var id int
			return d.QueryRowContext(ctx, "INSERT INTO records VALUES(2,'late row') RETURNING id").Scan(&id)
		}},
		{"ComponentData", func(_ context.Context, d *dml.Data) error {
			return d.ComponentData(dml.ComponentImperative, "").Execute("INSERT INTO records VALUES(2,'late component')")
		}},
	}
	for _, c := range calls {
		for _, caller := range []bool{false, true} {
			for _, borrowed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/caller=%t/borrowed=%t", c.name, caller, borrowed), func(t *testing.T) {
					ctx := t.Context()
					db := sqlite.New(t)
					if e := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END", "CREATE TABLE sqlx_sequence_reservations(table_name TEXT PRIMARY KEY,value INTEGER NOT NULL CHECK(value>=0))"); e != nil {
						t.Fatal(e)
					}
					source := dml.Source{DB: db.DB}
					if caller {
						var e error
						source.Tx, e = db.DB.BeginTx(ctx, nil)
						if e != nil {
							t.Fatal(e)
						}
						t.Cleanup(func() { _ = source.Tx.Rollback() })
						if _, e = source.Tx.ExecContext(ctx, "INSERT INTO records VALUES(99,'prior')"); e != nil {
							t.Fatal(e)
						}
					}
					out := &terminal49Output{t: t, call: c.call, useBorrowed: borrowed}
					result, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: true, DataSource: source, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
						out.root = mainScope(ctx)
						v, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
						if e != nil || !ok {
							return nil, e
						}
						out.native = out.root.data.(*dml.Data)
						out.borrowed = out.native.ComponentData(dml.ComponentImperative, "").(*dml.Data)
						out.borrowed.SealComponent()
						if e = out.native.Start(ctx); e != nil {
							return nil, e
						}
						_, out.tx = out.native.InvocationTransaction()
						if e = v.(xh.Data).Execute("INSERT INTO records VALUES(1,'admitted')"); e != nil {
							return nil, e
						}
						return out, nil
					})})
					if out.calls != 1 {
						t.Fatalf("terminal callback count=%d", out.calls)
					}
					expectedBefore := []int{1}
					if caller {
						expectedBefore = []int{99}
					}
					if !reflect.DeepEqual(out.before, expectedBefore) {
						t.Fatalf("wrong preparation stage rows=%v want=%v", out.before, expectedBefore)
					}
					if err == nil || result != nil {
						t.Errorf("DESIRED_NATIVE_TERMINAL_STICKY_GAP operation=%s error=%v result=%T", c.name, err, result)
					}
					if caller {
						outc := out.native.TransactionOutcome()
						if outc.State != xh.TransactionCallerPending {
							t.Fatalf("caller native outcome=%+v", outc)
						}
						// Caller-local prepare must remain skipped; rejection must not allow the
						// framework to execute the admitted queue after ignored terminal failure.
						if ids := terminal49IDs(t, source.Tx); !reflect.DeepEqual(ids, []int{99}) {
							t.Errorf("DESIRED_NATIVE_TERMINAL_CALLER_GAP rows=%v want prior99", ids)
						}
						if _, e := source.Tx.ExecContext(ctx, "INSERT INTO records VALUES(101,'caller usable')"); e != nil {
							t.Fatalf("caller TX unusable: %v", e)
						}
						if e := source.Tx.Rollback(); e != nil {
							t.Fatal(e)
						}
					} else {
						if outc := out.native.TransactionOutcome(); outc.State != xh.TransactionRolledBack {
							t.Errorf("DESIRED_NATIVE_TERMINAL_OUTCOME_GAP state=%s", outc.State)
						}
						if _, e := out.tx.ExecContext(ctx, "INSERT INTO records VALUES(101,'closed')"); !errors.Is(e, sql.ErrTxDone) {
							t.Fatalf("owned preparedTX remains live=%v", e)
						}
					}
					ids := terminal49IDs(t, db.DB)
					if len(ids) != 0 {
						t.Errorf("DESIRED_NATIVE_TERMINAL_PHYSICAL_GAP rows=%v", ids)
					}
					var audit, allocator int
					if e := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&audit); e != nil {
						t.Fatal(e)
					}
					if e := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlx_sequence_reservations").Scan(&allocator); e != nil {
						t.Fatal(e)
					}
					if audit != 0 || allocator != 0 {
						t.Errorf("DESIRED_NATIVE_TERMINAL_GUARD_GAP audit=%d allocator=%d", audit, allocator)
					}
					t.Logf("REAL_CLOSED_NATIVE_ATTEMPT operation=%s caller=%t borrowed=%t attempt=%v root=%v before=%v after=%v actualOutcome=%s physical=%v", c.name, caller, borrowed, out.attempt, err, out.before, out.after, out.native.TransactionOutcome().State, ids)
				})
			}
		}
	}
}
