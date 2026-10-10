package engine

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/sqlx/testutil/sqlfault"
	xh "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

func TestServerExecutionGuardNativeCommitFence(t *testing.T) {
	for _, scenario := range []string{"admitted", "initial-denial", "revoked-during-native-write", "expires-during-native-write"} {
		t.Run(scenario, func(t *testing.T) {
			db := sqlite.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id); END"); err != nil {
				t.Fatal(err)
			}
			denied := errors.New("owned source authority denied")
			revoked := scenario == "initial-denial"
			checks, calls, sqlCalls := 0, 0, 0
			nativeDB := db.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if call.Phase == "prepare" && strings.Contains(call.SQL, "INSERT INTO records") {
					sqlCalls++
					if scenario == "revoked-during-native-write" {
						revoked = true
					}
					if scenario == "expires-during-native-write" {
						cancel()
					}
				}
				return nil
			})
			ctx = rh.WithServerExecutionGuard(ctx, func(ctx context.Context) error {
				checks++
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if revoked {
					return denied
				}
				return nil
			})
			handler := rh.HandlerFunc(func(ctx context.Context, invocation rh.Invocation) (any, error) {
				calls++
				value, _, err := invocation.Binder.Lookup(ctx, xh.DataKey)
				if err != nil {
					return nil, err
				}
				data := value.(xh.Data)
				if err = data.Execute("INSERT INTO records VALUES(1)"); err != nil {
					return nil, err
				}
				return struct{ ID int }{1}, nil
			})
			output, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), DataSource: dml.Source{DB: nativeDB}, Handler: handler})
			want := 0
			if scenario == "admitted" {
				want = 1
				if err != nil {
					t.Fatal(err)
				}
				if checks < 2 {
					t.Fatalf("missing native guard recheck: %d", checks)
				}
			} else {
				if err == nil {
					t.Fatal("denied invocation succeeded")
				}
				if output != nil {
					t.Fatalf("denied result released: %#v", output)
				}
			}
			if scenario != "initial-denial" && sqlCalls == 0 {
				t.Fatal("native SQL never reached")
			}
			if scenario == "initial-denial" && calls != 0 {
				t.Fatal("unauthorized handler invoked")
			}
			db.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM records"}, []struct{ Total int }{{want}})
			db.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM audit"}, []struct{ Total int }{{want}})
		})
	}
}

func TestServerExecutionGuardSourceLessReadRelease(t *testing.T) {
	revoked := false
	ctx := rh.WithServerExecutionGuard(context.Background(), func(context.Context) error {
		if revoked {
			return errors.New("revoked")
		}
		return nil
	})
	out, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), Handler: rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { revoked = true; return "private-result", nil })})
	if err == nil || out != nil {
		t.Fatalf("source-less read released after revoke: out=%v err=%v", out, err)
	}
}
