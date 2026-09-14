package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/provider"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

func TestInjectedTransactionStarterSharesDownstreamSQLite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			commits := 0
			source := sqldml.Source{DB: h.DB, OnCommit: func(context.Context) { commits++ }}
			input := testRouteInput(t, reflect.TypeOf(struct{}{}))
			var initialize func(context.Context, rhandler.Invocation, int) error
			initialize = func(ctx context.Context, invocation rhandler.Invocation, id int) error {
				var dependencies struct {
					Starter xhandler.TransactionStarter `bind:"kind=transactionStarter,required"`
					DML     xhandler.DML                `bind:"kind=dml,required"`
				}
				if err := invocation.Binder.Bind(ctx, &dependencies); err != nil {
					return err
				}
				if _, exposed := dependencies.Starter.(interface {
					Complete(context.Context, error) error
				}); exposed {
					return errors.New("starter exposed completion")
				}
				if err := dependencies.Starter.Start(ctx); err != nil {
					return err
				}
				if err := dependencies.Starter.Start(ctx); err != nil {
					return err
				}
				return dependencies.DML.Execute("INSERT INTO records VALUES(?)", id)
			}
			_, err := New().Execute(ctx, Request{Input: input, BoundInput: &struct{}{}, DataSource: source,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					if err := initialize(ctx, invocation, 1); err != nil {
						return nil, err
					}
					_, err := New().Execute(ctx, Request{Input: input, BoundInput: &struct{}{}, DataSource: source, Handler: rhandler.HandlerFunc(func(ctx context.Context, child rhandler.Invocation) (any, error) {
						return nil, initialize(ctx, child, 2)
					})})
					if err != nil {
						return nil, err
					}
					if commits != 0 {
						return nil, errors.New("downstream completed owner transaction")
					}
					if fail {
						return nil, errors.New("root failed")
					}
					return nil, nil
				}),
			})
			if (err != nil) != fail {
				t.Fatalf("result error=%v", err)
			}
			wantCount, wantCommits := 2, 1
			if fail {
				wantCount, wantCommits = 0, 0
			}
			if commits != wantCommits {
				t.Fatalf("commits=%d want=%d", commits, wantCommits)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS total FROM records"}, []struct {
				Total int `sqlx:"total"`
			}{{wantCount}})
		})
	}
}

func TestTransactionStarterAuthorityIsProtected(t *testing.T) {
	fake := []locator.Provider{provider.Static(xhandler.TransactionStarterKey, struct{}{})}
	for _, input := range []providerComposition{{component: fake}, {protocol: fake}, {child: fake}} {
		if _, err := (providerComposer{}).compose(input); err == nil {
			t.Fatal("accepted untrusted transaction starter")
		}
	}
}
