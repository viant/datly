package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/sqlx/testutil/sqlfault"
	xh "github.com/viant/xdatly/handler"
)

func TestServerExecutionGuardRevokedWhileNativeWriteWaiting(t *testing.T) {
	db := sqlite.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id); END"); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var revoked atomic.Bool
	nativeDB := db.FaultDB(t, func(ctx context.Context, call sqlfault.Call) error {
		if call.Phase == "prepare" && strings.Contains(call.SQL, "INSERT INTO records") {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	ctx = rh.WithServerExecutionGuard(ctx, func(context.Context) error {
		if revoked.Load() {
			return errors.New("owned authority revoked while native operation waited")
		}
		return nil
	})
	type outcome struct {
		value any
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), DataSource: dml.Source{DB: nativeDB}, Handler: rh.HandlerFunc(func(ctx context.Context, invocation rh.Invocation) (any, error) {
			value, _, err := invocation.Binder.Lookup(ctx, xh.DataKey)
			if err != nil {
				return nil, err
			}
			if err = value.(xh.Data).Execute("INSERT INTO records VALUES(1)"); err != nil {
				return nil, err
			}
			return "must-not-release", nil
		})})
		done <- outcome{value, err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("native write never reached owned wait barrier")
	}
	revoked.Store(true)
	close(release)
	select {
	case got := <-done:
		if got.err == nil || got.value != nil {
			t.Fatalf("revoked native operation released output: %v %v", got.value, got.err)
		}
	case <-ctx.Done():
		t.Fatal("native operation did not complete")
	}
	db.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM records"}, []struct{ Total int }{{0}})
	db.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{0}})
}

func TestServerExecutionGuardDeferredConnectorOwner(t *testing.T) {
	for _, deny := range []bool{false, true} {
		t.Run(map[bool]string{false: "admitted", true: "revoked-before-commit"}[deny], func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id); END"); err != nil {
				t.Fatal(err)
			}
			connectors := &dsql.SQLComponent{}
			if err := connectors.RegisterConnector("owned", db.DB); err != nil {
				t.Fatal(err)
			}
			var revoked atomic.Bool
			ctx = rh.WithServerExecutionGuard(ctx, func(context.Context) error {
				if revoked.Load() {
					return errors.New("owned source revoked")
				}
				return nil
			})
			value, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), Capabilities: rh.InvocationCapabilities{Connector: connectors}, Handler: rh.HandlerFunc(func(ctx context.Context, invocation rh.Invocation) (any, error) {
				capability, found, err := invocation.Binder.Lookup(ctx, rh.TransactionSQLCapabilityKey)
				if err != nil || !found {
					return nil, errors.New("missing actual connector transaction")
				}
				sql, err := capability.(rh.TransactionSQLProvider).Connector(ctx, "owned")
				if err != nil {
					return nil, err
				}
				if _, err = sql.ExecContext(ctx, "INSERT INTO records VALUES(1)"); err != nil {
					return nil, err
				}
				revoked.Store(deny)
				return "owned-result", nil
			})})
			want := 1
			if deny {
				want = 0
				if err == nil || value != nil {
					t.Fatalf("revoked connector released result: %v %v", value, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			db.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM records"}, []struct{ Total int }{{want}})
			db.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{want}})
		})
	}
}

func TestServerExecutionGuardRejectsUnsupportedActualOwner(t *testing.T) {
	ctx := rh.WithServerExecutionGuard(context.Background(), func(context.Context) error { return nil })
	probe := &guardProbe{}
	called := false
	value, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), DataSource: &staticDataSource{data: probe}, Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { called = true; return "must-not-release", nil })})
	if err == nil || value != nil || called || probe.mutations != 0 {
		t.Fatalf("unsupported owner accepted: result=%v err=%v called=%v mutations=%d", value, err, called, probe.mutations)
	}
}
