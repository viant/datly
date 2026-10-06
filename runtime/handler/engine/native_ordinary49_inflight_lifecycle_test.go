package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

// This is separate concurrent work during an actual ordinary native drain. No
// lifecycle method is called synchronously inside the held OnCommit callback.
func TestNativeOrdinary49InFlightLifecycleSerialization(t *testing.T) {
	for _, api := range []string{"BeginInvocation", "ValidateExecutionGuards", "CloseMutationAdmission"} {
		for _, borrowed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/borrowed=%t", api, borrowed), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				db := sqlite.New(t)
				if e := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"); e != nil {
					t.Fatal(e)
				}
				entered, release, expired := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
				var once sync.Once
				releaseCallback := func() { once.Do(func() { close(release) }) }
				defer releaseCallback()
				var actual, target *dml.Data
				source := dml.Source{DB: db.DB, OnCommit: func(callbackCtx context.Context) {
					close(entered)
					select {
					case <-release:
					case <-callbackCtx.Done():
						expired <- struct{}{}
					}
				}}
				type completion struct {
					result any
					err    error
				}
				engineDone := make(chan completion, 1)
				go func() {
					result, e := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), DataSource: source, Handler: rh.HandlerFunc(func(ctx context.Context, in rh.Invocation) (any, error) {
						value, ok, e := in.Binder.Lookup(ctx, xh.DataKey)
						if e != nil || !ok {
							return nil, fmt.Errorf("genuine ordinary Data found=%t: %w", ok, e)
						}
						actual = mainScope(ctx).data.(*dml.Data)
						target = actual
						if borrowed {
							target = actual.ComponentData(dml.ComponentImperative, "").(*dml.Data)
							target.SealComponent()
						}
						if e = value.(xh.Data).Execute("INSERT INTO records VALUES(1)"); e != nil {
							return nil, e
						}
						return "ordinary-success", nil
					})})
					engineDone <- completion{result, e}
				}()
				select {
				case <-entered:
				case result := <-engineDone:
					t.Fatalf("actual held commit callback not reached: %+v", result)
				case <-ctx.Done():
					t.Fatal("actual callback stage timeout")
				}
				if actual == nil || target == nil {
					t.Fatal("actual Source.Open owner unavailable")
				}
				if drainowner.OwnerActivitiesEnrolled(actual) || drainowner.ProtectedOwnerFailure(actual) != nil {
					t.Fatal("ordinary invocation acquired protected policy")
				}
				if e := drainowner.CheckProtectedDrainInFlight(actual); e != nil {
					t.Fatalf("actual never-enrolled drain precheck rejected: %v", e)
				}
				for _, table := range []string{"records", "audit"} {
					var n int
					if e := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); e != nil || n != 1 {
						t.Fatalf("actual physical commit before callback release %s=%d error=%v", table, n, e)
					}
				}
				started, methodDone := make(chan struct{}), make(chan error, 1)
				go func() {
					close(started)
					var e error
					switch api {
					case "BeginInvocation":
						e = target.BeginInvocation()
					case "ValidateExecutionGuards":
						e = target.ValidateExecutionGuards(ctx)
					case "CloseMutationAdmission":
						e = target.CloseMutationAdmission()
					}
					methodDone <- e
				}()
				<-started
				select {
				case e := <-methodDone:
					t.Fatalf("ordinary lifecycle did not retain execution serialization while callback held: %v", e)
				case <-time.After(40 * time.Millisecond):
				}
				// This pre-release observation is a real concurrent call, while the Engine
				// has not returned and the native completion callback remains held.
				select {
				case result := <-engineDone:
					t.Fatalf("native drain ended before release: %+v", result)
				default:
				}
				if drainowner.OwnerActivitiesEnrolled(actual) || drainowner.ProtectedOwnerFailure(actual) != nil {
					t.Fatal("ordinary concurrent work enrolled or latched protection")
				}
				t.Logf("GENUINE_ORDINARY_DRAIN_IN_FLIGHT api=%s borrowed=%t physicalRecords=1 audit=1 methodBlockedOnExistingSerialization=true protected=false", api, borrowed)
				releaseCallback()
				var result completion
				select {
				case result = <-engineDone:
				case <-ctx.Done():
					t.Fatal("ordinary Engine did not complete after callback release")
				}
				var methodError error
				select {
				case methodError = <-methodDone:
				case <-ctx.Done():
					t.Fatal("ordinary lifecycle stayed blocked after native drain completed")
				}
				if result.err != nil || result.result != "ordinary-success" {
					t.Fatalf("ordinary result changed: %+v", result)
				}
				if errors.Is(methodError, drainowner.ErrDrain) || errors.Is(methodError, drainowner.ErrDrainOverlap) {
					t.Fatalf("ordinary lifecycle acquired protected rejection: %v", methodError)
				}
				if api == "BeginInvocation" {
					if !errors.Is(methodError, dml.ErrInvocationCompleted) {
						t.Fatalf("existing completed BeginInvocation result changed: %v", methodError)
					}
				} else if methodError != nil {
					t.Fatalf("existing ordinary lifecycle result changed: %v", methodError)
				}
				select {
				case <-expired:
					t.Fatal("callback was released by deadline rather than test barrier")
				default:
				}
				if drainowner.OwnerActivitiesEnrolled(actual) || drainowner.ProtectedOwnerFailure(actual) != nil {
					t.Fatal("ordinary completion acquired protected enrollment/failure")
				}
				outcome := actual.TransactionOutcome()
				if outcome.State != xh.TransactionCommitted || outcome.Error != nil {
					t.Fatalf("truthful ordinary committed outcome changed: %+v", outcome)
				}
				for _, table := range []string{"records", "audit"} {
					var n int
					if e := db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); e != nil || n != 1 {
						t.Fatalf("actual physical commit changed %s=%d error=%v", table, n, e)
					}
				}
				_, tx := actual.InvocationTransaction()
				if tx == nil {
					t.Fatal("missing real completed transaction")
				}
				if _, e := tx.ExecContext(ctx, "INSERT INTO records VALUES(2)"); !errors.Is(e, sql.ErrTxDone) {
					t.Fatalf("ordinary committed transaction reopened: %v", e)
				}
				t.Logf("GENUINE_ORDINARY_DRAIN_COMPLETE api=%s borrowed=%t methodResult=%v EngineSuccess=true actualOutcome=%s physicalRecords=1 audit=1 protected=false", api, borrowed, methodError, outcome.State)
			})
		}
	}
}
