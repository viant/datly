package engine

import (
	"context"
	"database/sql"
	"errors"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

type protectedErrorOutput struct {
	root         *dataScope
	calls        int
	seen         error
	veto         error
	tx           *sql.Tx
	wantPrepared bool
	t            *testing.T
}

func (o *protectedErrorOutput) Finalize(ctx context.Context, cause error) error {
	o.calls++
	o.seen = cause
	if !o.root.completionStarted {
		o.t.Fatal("terminal observation preceded admission closure")
	}
	if check := drainowner.CheckActivities(o.root.nativeInvocation); errors.Is(check, drainowner.ErrActivityUnfinished) {
		o.t.Fatalf("terminal observation retained composition activity: %v", check)
	}
	if _, err := drainowner.AdmitActivity(o.root.nativeInvocation); !errors.Is(err, drainowner.ErrActivityClosed) {
		o.t.Fatalf("terminal activity admission open: %v", err)
	}
	if cause == nil {
		var count int
		if err := o.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id=2").Scan(&count); err != nil {
			o.t.Fatal(err)
		}
		want := 0
		if o.wantPrepared {
			want = 1
		}
		if count != want {
			o.t.Fatalf("local/caller preparation ordering count=%d want=%d", count, want)
		}
	}
	return o.veto
}
func TestProtectedErrorFinalizerObservesClosedPreparedRootOnce(t *testing.T) {
	for _, mode := range []string{"local", "caller", "local prepare failure", "caller prepare failure", "local veto"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			caller := strings.HasPrefix(mode, "caller")
			bad := strings.Contains(mode, "failure")
			source := dml.Source{DB: db.DB}
			if caller {
				tx, err := db.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				source.Tx = tx
				if _, err = tx.ExecContext(ctx, "INSERT INTO records VALUES(99)"); err != nil {
					t.Fatal(err)
				}
			}
			out := &protectedErrorOutput{t: t, wantPrepared: !caller}
			if mode == "local veto" {
				out.veto = errors.New("terminal business veto")
			}
			result, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, BufferedComponentCalls: true,
				Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
					out.root = mainScope(ctx)
					value, found, e := in.Binder.Lookup(ctx, xh.DataKey)
					if e != nil || !found {
						t.Fatalf("data lookup found=%v err=%v", found, e)
					}
					native := out.root.data.(*dml.Data)
					if e = native.Start(ctx); e != nil {
						return nil, e
					}
					_, out.tx = native.InvocationTransaction()
					query := "INSERT INTO records VALUES(2)"
					if bad {
						query = "INSERT INTO missing_protected_table VALUES(2)"
					}
					if e = value.(xh.Data).Execute(query); e != nil {
						return nil, e
					}
					return out, nil
				}),
			})
			if out.calls != 1 {
				t.Fatalf("terminal calls=%d", out.calls)
			}
			if !caller && bad {
				if err == nil || out.seen == nil || result != nil {
					t.Fatalf("prepare failure err=%v seen=%v result=%T", err, out.seen, result)
				}
			} else if caller && bad {
				if err == nil || out.seen != nil || result != nil {
					t.Fatalf("caller should prepare after terminal: err=%v seen=%v result=%T", err, out.seen, result)
				}
			} else if out.veto != nil {
				if !errors.Is(err, out.veto) || result != out {
					t.Fatalf("business veto err=%v result=%T", err, result)
				}
			} else if err != nil || result != out {
				t.Fatalf("result=%T err=%v", result, err)
			}
			outcome := out.root.data.(*dml.Data).TransactionOutcome().State
			if caller {
				if outcome != xh.TransactionCallerPending {
					t.Fatalf("caller outcome=%v", outcome)
				}
				var n int
				if e := source.Tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id=99").Scan(&n); e != nil || n != 1 {
					t.Fatalf("caller prior count=%d err=%v", n, e)
				}
			} else {
				want := 0
				if !bad && out.veto == nil {
					want = 1
				}
				var n int
				if e := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&n); e != nil || n != want {
					t.Fatalf("physical count=%d want=%d err=%v", n, want, e)
				}
				state := xh.TransactionRolledBack
				if want == 1 {
					state = xh.TransactionCommitted
				}
				if outcome != state {
					t.Fatalf("outcome=%v want=%v", outcome, state)
				}
			}
		})
	}
}

type protectedSuccessOutput struct {
	commits *int
	calls   int
	t       *testing.T
}

func (o *protectedSuccessOutput) Finalize(context.Context) error {
	o.calls++
	if *o.commits != 1 {
		o.t.Fatal("success hook did not run after native commit")
	}
	return nil
}
func TestProtectedSuccessHookStillRunsAfterCommit(t *testing.T) {
	ctx := t.Context()
	db := testharness.NewSQLiteHarness(t)
	if e := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); e != nil {
		t.Fatal(e)
	}
	commits := 0
	out := &protectedSuccessOutput{commits: &commits, t: t}
	result, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: true, DataSource: dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
		value, _, e := in.Binder.Lookup(ctx, xh.DataKey)
		if e != nil {
			return nil, e
		}
		return out, value.(xh.Data).Execute("INSERT INTO records VALUES(1)")
	})})
	if err != nil || result != out || out.calls != 1 || commits != 1 {
		t.Fatalf("err=%v result=%T hook=%d commits=%d", err, result, out.calls, commits)
	}
}
