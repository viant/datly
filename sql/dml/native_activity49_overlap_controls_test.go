package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx/testutil/sqlfault"
	xhandler "github.com/viant/xdatly/handler"
)

// These witnesses use native drain APIs and real SQLite journal/transaction
// state. Exact Claim/Attach binds the existing owner; no permit or activity is
// manufactured, and no synthetic callback count is transaction evidence.
type overlap49Fixture struct {
	h             *sqlite.Harness
	native, child *Data
	issuer        *drainowner.Invocation
	handle        drainowner.Handle
	caller        *sql.Tx
}

func overlap49New(t *testing.T, external bool, before func(context.Context, sqlfault.Call) error) *overlap49Fixture {
	t.Helper()
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(),
		"CREATE TABLE records(id INTEGER PRIMARY KEY)",
		"CREATE TABLE audit(id INTEGER)",
		"CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(new.id);END"); err != nil {
		t.Fatal(err)
	}
	db := h.DB
	if before != nil {
		db = h.FaultDB(t, before)
	}
	f := &overlap49Fixture{h: h, issuer: drainowner.NewInvocation()}
	var opts []Option
	if external {
		var err error
		f.caller, err = db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.caller.Rollback() })
		opts = append(opts, WithTx(f.caller))
	}
	f.native = NewData(db, opts...)
	handle, err := drainowner.Claim(f.native, f.issuer)
	if err != nil {
		t.Fatal(err)
	}
	f.handle = handle
	if err = handle.Attach(f.native, f.issuer); err != nil {
		t.Fatal(err)
	}
	if !handle.Attached(f.native, f.issuer) {
		t.Fatal("no genuine native attachment")
	}
	if err = f.native.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = f.native.Execute("INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	f.child = f.native.ComponentData(ComponentImperative, "").(*Data)
	if err = f.child.Execute("INSERT INTO records VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	return f
}
func overlap49Run(ctx context.Context, d *Data, api string) error {
	switch api {
	case "Flush":
		return d.Flush(ctx, "")
	case "PrepareFinalization":
		return d.PrepareFinalization(ctx)
	case "PrepareCompletion":
		return d.PrepareCompletion(ctx)
	case "Complete":
		return d.Complete(ctx, nil)
	}
	panic(api)
}

type overlap49JournalFact struct {
	ID                 uint64
	SQL                string
	Executed, Reserved bool
}

func overlap49Journal(d *Data) []overlap49JournalFact {
	var facts []overlap49JournalFact
	for _, op := range flattenData(d) {
		facts = append(facts, overlap49JournalFact{op.id, op.dml, op.executed, op.reserved})
	}
	return facts
}
func overlap49Count(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) int {
	t.Helper()
	var n int
	if err := q.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM audit").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func overlap49Observe(t *testing.T, f *overlap49Fixture) int {
	t.Helper()
	if f.caller != nil {
		return overlap49Count(t, f.caller)
	}
	if !f.native.completed && f.native.tx != nil {
		return overlap49Count(t, f.native.tx)
	}
	return overlap49Count(t, f.h.DB)
}
func overlap49CallerStillOwns(t *testing.T, f *overlap49Fixture) {
	t.Helper()
	if f.caller == nil {
		return
	}
	if _, err := f.caller.ExecContext(context.Background(), "INSERT INTO records VALUES(99)"); err != nil {
		t.Fatalf("framework completed caller transaction: %v", err)
	}
	if err := f.caller.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := overlap49Count(t, f.h.DB); n != 0 {
		t.Fatalf("caller rollback left rows=%d", n)
	}
}

// An actual driver callback pauses inside native executionMu, before the real
// INSERT is prepared. Enrollment must never wait on that callback's release.
func TestNativeActivity49DesiredOrdinaryDrainOverlap(t *testing.T) {
	for _, api := range []string{"Flush", "PrepareFinalization", "PrepareCompletion", "Complete"} {
		for _, external := range []bool{false, true} {
			if external && api == "PrepareFinalization" {
				continue
			} // documented caller-local skip; cannot be a live SQL callback
			for _, borrowed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/caller=%t/borrowed=%t", api, external, borrowed), func(t *testing.T) {
					entered, release := make(chan struct{}), make(chan struct{})
					var once sync.Once
					f := overlap49New(t, external, func(ctx context.Context, call sqlfault.Call) error {
						if call.Phase == "prepare" && strings.HasPrefix(call.SQL, "INSERT INTO records") {
							once.Do(func() {
								close(entered)
								select {
								case <-release:
								case <-ctx.Done():
								}
							})
						}
						return nil
					})
					d := f.native
					if borrowed {
						d = f.child
					}
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					done := make(chan error, 1)
					go func() { done <- overlap49Run(ctx, d, api) }()
					select {
					case <-entered:
					case err := <-done:
						t.Fatalf("intended SQL callback not reached: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					// This is a separate synchronous bookkeeping operation, never a fake token.
					enroll := make(chan error, 1)
					go func() { enroll <- drainowner.EnrollActivities(f.issuer) }()
					var enrollment error
					prompt := true
					select {
					case enrollment = <-enroll:
					case <-time.After(250 * time.Millisecond):
						prompt = false
					}
					close(release)
					var drainErr error
					select {
					case drainErr = <-done:
					case <-ctx.Done():
						t.Fatal("native drain selfwait", ctx.Err())
					}
					if !prompt {
						select {
						case enrollment = <-enroll:
						case <-ctx.Done():
							t.Fatal("enrollment callback selfwait", ctx.Err())
						}
					}
					sticky := drainowner.CheckActivities(f.issuer)
					rows := overlap49Observe(t, f)
					outcome := f.native.TransactionOutcome()
					t.Logf("REAL_ORDINARY_DRAIN_OVERLAP api=%s caller=%t borrowed=%t prompt=%t enrollErr=%v sticky=%v drainErr=%v rows=%d completed=%t outcome=%s journal=%+v", api, external, borrowed, prompt, enrollment, sticky, drainErr, rows, f.native.completed, outcome.State, overlap49Journal(f.native))
					// Cleanup cannot rewrite an already committed/caller-pending first outcome.
					if !f.native.completed {
						_ = f.handle.Call(ctx, f.native, f.issuer, drainowner.Abort, errors.New("explicit witness cleanup"))
					}
					overlap49CallerStillOwns(t, f)
					if !prompt {
						t.Error("DESIRED_OVERLAP_GAP: enrollment waited on active native callback")
					}
					if enrollment == nil {
						t.Error("DESIRED_OVERLAP_GAP: protection enrolled across active ordinary native drain")
					}
					if sticky == nil {
						t.Error("DESIRED_OVERLAP_GAP: ignored overlap did not latch protection failure")
					}
					if external && f.native.TransactionOutcome().State != xhandler.TransactionCallerPending {
						t.Errorf("caller outcome rewritten: %+v", f.native.TransactionOutcome())
					}
				})
			}
		}
	}
}

// Protection-first public root and borrowed APIs must reject before reserving
// journal operations, sealing/completing ownership, or preparing/draining SQL.
func TestNativeActivity49DesiredProtectedPublicDrain(t *testing.T) {
	for _, api := range []string{"Flush", "PrepareFinalization", "PrepareCompletion", "Complete"} {
		for _, external := range []bool{false, true} {
			for _, borrowed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/caller=%t/borrowed=%t", api, external, borrowed), func(t *testing.T) {
					f := overlap49New(t, external, nil)
					if err := drainowner.EnrollActivities(f.issuer); err != nil {
						t.Fatal(err)
					}
					before := overlap49Journal(f.native)
					beforeTx := f.native.tx
					d := f.native
					if borrowed {
						d = f.child
					}
					attempt := overlap49Run(context.Background(), d, api)
					after := overlap49Journal(f.native)
					rows := overlap49Observe(t, f)
					changed := !reflect.DeepEqual(before, after) || f.native.completed || f.native.tx != beforeTx
					sticky := drainowner.CheckActivities(f.issuer)
					// Deliberately ignore the public attempt: a successful completion must
					// remain vetoed by the latched attempt, without undoing caller ownership.
					ignoredCompletion := f.native.Complete(context.Background(), nil)
					finalRows := overlap49Observe(t, f)
					outcome := f.native.TransactionOutcome()
					t.Logf("REAL_PROTECTION_FIRST api=%s caller=%t borrowed=%t attempt=%v sticky=%v ownershipChanged=%t before=%+v after=%+v rows=%d ignoredComplete=%v finalRows=%d outcome=%s", api, external, borrowed, attempt, sticky, changed, before, after, rows, ignoredCompletion, finalRows, outcome.State)
					overlap49CallerStillOwns(t, f)
					if attempt == nil {
						t.Error("DESIRED_PROTECTED_DRAIN_GAP: protected public drain succeeded")
					}
					if sticky == nil {
						t.Error("DESIRED_PROTECTED_DRAIN_GAP: ignored public attempt did not latch")
					}
					if changed || rows != 0 {
						t.Errorf("DESIRED_PROTECTED_DRAIN_GAP: first attempt changed journal/ownership/SQL rows=%d", rows)
					}
					if ignoredCompletion == nil || finalRows != 0 {
						t.Errorf("DESIRED_PROTECTED_DRAIN_GAP: ignored attempt permitted completion=%v rows=%d", ignoredCompletion, finalRows)
					}
					if external && outcome.State != xhandler.TransactionCallerPending {
						t.Errorf("caller ownership outcome=%s", outcome.State)
					}
				})
			}
		}
	}
}

func TestNativeActivity49CompletedOrdinaryDrainControl(t *testing.T) {
	for _, api := range []string{"Flush", "PrepareFinalization", "PrepareCompletion"} {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/caller=%t", api, external), func(t *testing.T) {
				f := overlap49New(t, external, nil)
				if err := overlap49Run(context.Background(), f.native, api); err != nil {
					t.Fatal(err)
				}
				rows := overlap49Observe(t, f)
				want := 2
				if api == "Flush" {
					want = 1 // Root-target causal flush does not cross its open child marker.
				}
				if external && api == "PrepareFinalization" {
					want = 0
				}
				if rows != want {
					t.Fatalf("ordinary documented drain rows=%d want=%d", rows, want)
				}
				if err := drainowner.EnrollActivities(f.issuer); err != nil {
					t.Fatalf("completed ordinary operation poisoned later protection: %v", err)
				}
				if err := drainowner.CheckActivities(f.issuer); err != nil {
					t.Fatalf("completed operation wrongly overlaps: %v", err)
				}
				cause := errors.New("protected root cleanup cause")
				if err := f.handle.Call(context.Background(), f.native, f.issuer, drainowner.Abort, cause); !errors.Is(err, cause) {
					t.Fatalf("abort cause lost: %v", err)
				}
				outcome := f.native.TransactionOutcome()
				wantState := xhandler.TransactionRolledBack
				if external {
					wantState = xhandler.TransactionCallerPending
				}
				if outcome.State != wantState {
					t.Fatalf("truthful first outcome=%s want=%s", outcome.State, wantState)
				}
				t.Logf("REAL_COMPLETED_ORDINARY_CONTROL api=%s caller=%t preEnrollmentRows=%d actualOutcome=%s", api, external, rows, outcome.State)
				overlap49CallerStillOwns(t, f)
				if !external && overlap49Count(t, f.h.DB) != 0 {
					t.Fatal("owned cleanup failed to rollback")
				}
			})
		}
	}
}

// Enrollment is also invoked directly by the real synchronous driver callback.
// It must reject without acquiring executionMu already held by its caller.
func TestNativeActivity49DesiredCallbackEnrollment(t *testing.T) {
	for _, api := range []string{"Flush", "PrepareFinalization", "PrepareCompletion", "Complete"} {
		t.Run(api, func(t *testing.T) {
			var f *overlap49Fixture
			var enrollment error
			var elapsed time.Duration
			reached := false
			var once sync.Once
			f = overlap49New(t, false, func(_ context.Context, call sqlfault.Call) error {
				if call.Phase == "prepare" && strings.HasPrefix(call.SQL, "INSERT INTO records") {
					once.Do(func() {
						reached = true
						start := time.Now()
						enrollment = drainowner.EnrollActivities(f.issuer)
						elapsed = time.Since(start)
					})
				}
				return nil
			})
			drainErr := overlap49Run(context.Background(), f.native, api)
			if !reached {
				t.Fatalf("intended direct callback not reached: %v", drainErr)
			}
			sticky := drainowner.CheckActivities(f.issuer)
			rows := overlap49Observe(t, f)
			t.Logf("REAL_REENTRANT_ENROLLMENT api=%s elapsed=%s enrollment=%v sticky=%v drainErr=%v rows=%d outcome=%s", api, elapsed, enrollment, sticky, drainErr, rows, f.native.TransactionOutcome().State)
			if !f.native.completed {
				_ = f.handle.Call(context.Background(), f.native, f.issuer, drainowner.Abort, errors.New("explicit callback witness cleanup"))
			}
			if elapsed > 250*time.Millisecond {
				t.Errorf("DESIRED_CALLBACK_GAP: callback enrollment blocked %s", elapsed)
			}
			if enrollment == nil {
				t.Error("DESIRED_CALLBACK_GAP: synchronous ordinary drain callback enrolled protection")
			}
			if sticky == nil {
				t.Error("DESIRED_CALLBACK_GAP: synchronous overlap not latched")
			}
		})
	}
}
