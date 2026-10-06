package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

func TestProtectedCompletionRetainsOneNativeSnapshotAndRejectsLiveActivity(t *testing.T) {
	for _, early := range []bool{false, true} {
		name := "normal"
		if early {
			name = "unfinished"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			other := testharness.NewSQLiteHarness(t)
			var root *dataScope
			var snapshot []*dataScope
			var earlyErr error
			_, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), BufferedComponentCalls: true, DataSource: dml.Source{DB: db.DB}, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
				root = mainScope(ctx)
				data, found, e := in.Binder.Lookup(ctx, xh.DataKey)
				if e != nil || !found {
					t.Fatalf("data found=%v err=%v", found, e)
				}
				if e = data.(xh.Data).Execute("INSERT INTO records VALUES(1)"); e != nil {
					return nil, e
				}
				if early {
					snapshot, earlyErr = root.beginProtectedCompletion(ctx)
					if !errors.Is(earlyErr, drainowner.ErrActivityUnfinished) {
						t.Fatalf("live handler was not rejected: %v", earlyErr)
					}
					// Closure prevents more managed writes and source registration, but it
					// must not perform preparation or replace the handler's own activity.
					if e = data.(xh.Data).Execute("INSERT INTO records VALUES(2)"); e == nil {
						t.Fatal("write after admission closure succeeded")
					}
					if _, e = root.databaseUnit(ctx, dml.Source{DB: other.DB}, other.DB, ""); e == nil {
						t.Fatal("new source admitted after closure")
					}
					var count int
					if e = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); e != nil || count != 0 {
						t.Fatalf("barrier drained SQL count=%d err=%v", count, e)
					}
				}
				return "result", nil
			})})
			if early && !errors.Is(err, drainowner.ErrActivityUnfinished) {
				t.Fatalf("unfinished activity did not veto: %v", err)
			}
			if !early && err != nil {
				t.Fatal(err)
			}
			one, firstErr := root.beginProtectedCompletion(ctx)
			two, secondErr := root.beginProtectedCompletion(ctx)
			if len(one) != 1 || len(two) != 1 || &one[0] != &two[0] || firstErr != secondErr {
				t.Fatal("closure replaced retained unit snapshot or error")
			}
			if early && (&snapshot[0] != &one[0] || firstErr != earlyErr) {
				t.Fatal("completion did not reuse early closure snapshot")
			}
			if _, e := root.databaseUnit(ctx, dml.Source{DB: other.DB}, other.DB, ""); e == nil {
				t.Fatal("late source admitted after completion")
			}
			if _, e := drainowner.AdmitActivity(root.nativeInvocation); !errors.Is(e, drainowner.ErrActivityClosed) {
				t.Fatalf("activity admission reopened: %v", e)
			}
			var count int
			if e := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); e != nil {
				t.Fatal(e)
			}
			want := 1
			if early {
				want = 0
			}
			if count != want {
				t.Fatalf("native physical count=%d want=%d", count, want)
			}
			state := root.data.(*dml.Data).TransactionOutcome().State
			if !early && state != xh.TransactionCommitted {
				t.Fatalf("outcome=%v", state)
			}
			if early && state != xh.TransactionNone && state != xh.TransactionRolledBack {
				t.Fatalf("abort outcome=%v", state)
			}
		})
	}
}

func TestProtectedCompletionFailureSuppressesSuccessButPreservesBusinessOutput(t *testing.T) {
	for _, mode := range []string{"caught child", "business", "success"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			db := testharness.NewSQLiteHarness(t)
			if e := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)"); e != nil {
				t.Fatal(e)
			}
			contract := testRouteInput(t, reflect.TypeFor[struct{}]())
			cause := errors.New("native child or business failure")
			out := &struct{ Value string }{"successful handler result"}
			result, err := New().Execute(ctx, Request{Input: contract, DataSource: dml.Source{DB: db.DB}, BufferedComponentCalls: true, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
				cap, _, e := in.Binder.Lookup(ctx, xh.DataKey)
				if e != nil {
					return nil, e
				}
				if e = cap.(xh.Data).Execute("INSERT INTO records VALUES(1)"); e != nil {
					return nil, e
				}
				if mode == "caught child" {
					_, childErr := New().Execute(PrepareComponent(ctx, ComponentBufferedImperative, ""), Request{Input: contract, DataSource: dml.Source{DB: db.DB}, BufferedComponentCalls: true, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
						value, _, e := in.Binder.Lookup(ctx, xh.DataKey)
						if e != nil {
							return nil, e
						}
						if e = value.(xh.Data).Execute("INSERT INTO records VALUES(2)"); e != nil {
							return nil, e
						}
						return "child", cause
					})})
					if !errors.Is(childErr, cause) {
						t.Fatalf("child cause=%v", childErr)
					}
				}
				if mode == "business" {
					return out, cause
				}
				return out, nil
			})})
			if mode == "caught child" && (result != nil || !errors.Is(err, cause)) {
				t.Fatalf("completion failure retained success result=%T err=%v", result, err)
			}
			if mode == "business" && (result != out || !errors.Is(err, cause)) {
				t.Fatalf("business error lost output result=%T err=%v", result, err)
			}
			if mode == "success" && (result != out || err != nil) {
				t.Fatalf("success result=%T err=%v", result, err)
			}
			var count int
			if e := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&count); e != nil {
				t.Fatal(e)
			}
			want := 0
			if mode == "success" {
				want = 1
			}
			if count != want {
				t.Fatalf("physical rows=%d want=%d", count, want)
			}
		})
	}
}

func TestProtectedFreshFailureKeepsOriginalErrorAndCachedClosure(t *testing.T) {
	root := neutralDataScope()
	if err := root.enrollBufferedScope(); err != nil {
		t.Fatal(err)
	}
	units, err := root.beginProtectedCompletion(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	original := errors.New("original operation")
	if got := root.protectedCompletionFailure(original); got != original {
		t.Fatal("empty collector rewrote error identity")
	}
	late := errors.New("later protected failure")
	root.failGuardedExecution(late)
	got := root.protectedCompletionFailure(original)
	if !errors.Is(got, original) || !errors.Is(got, late) {
		t.Fatalf("failure lost: %v", got)
	}
	if again := root.protectedCompletionFailure(got); again != got {
		t.Fatal("fresh collector duplicated represented cause")
	}
	cached, cachedErr := root.beginProtectedCompletion(t.Context())
	if cachedErr != nil || &cached[0] != &units[0] {
		t.Fatal("fresh failure rewrote closure evidence")
	}
	if err := root.prepareProtectedFinalization(t.Context()); !errors.Is(err, late) {
		t.Fatalf("late failure admitted preparation: %v", err)
	}
	shared := errors.New("shared join branch")
	extra := errors.New("additional branch")
	combined := appendCompletionFailure(shared, errors.Join(shared, errors.Join(extra, shared)))
	if len(combined.(interface{ Unwrap() []error }).Unwrap()) != 2 {
		t.Fatalf("duplicate join branches: %v", combined)
	}
	wrapped := &publicOutcomeError{cause: shared}
	if got := appendCompletionFailure(nil, wrapped); got != wrapped {
		t.Fatal("intentional public error wrapper stripped")
	}
}
