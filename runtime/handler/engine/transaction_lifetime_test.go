package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

func TestChildFirstTransactionStartLifetimeSQLite(t *testing.T) {
	for _, mode := range []string{"child success", "root canceled", "child pre-canceled"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			source := sqldml.Source{DB: h.DB}
			input := testRouteInput(t, reflect.TypeOf(struct{}{}))
			rootBase, cancelRoot := context.WithCancel(t.Context())
			defer cancelRoot()
			childStarted := false
			_, err := New().Execute(rootBase, Request{Input: input, BoundInput: &struct{}{}, DataSource: source,
				Handler: rhandler.HandlerFunc(func(rootCtx context.Context, root rhandler.Invocation) (any, error) {
					childCtx, cancelChild := context.WithCancel(rootCtx)
					defer cancelChild()
					if mode == "child pre-canceled" {
						cancelChild()
					}
					_, err := New().Execute(childCtx, Request{Input: input, BoundInput: &struct{}{}, DataSource: source,
						Handler: rhandler.HandlerFunc(func(ctx context.Context, child rhandler.Invocation) (any, error) {
							var dependencies struct {
								Starter xhandler.TransactionStarter `bind:"kind=transactionStarter,required"`
								DML     xhandler.DML                `bind:"kind=dml,required"`
							}
							if err := child.Binder.Bind(ctx, &dependencies); err != nil {
								return nil, err
							}
							if err := dependencies.Starter.Start(ctx); err != nil {
								return nil, err
							}
							childStarted = true
							return nil, dependencies.DML.Execute("INSERT INTO records VALUES(1)")
						}),
					})
					cancelChild() // successful child return must not end the root transaction
					if err != nil {
						return nil, err
					}
					if mode == "root canceled" {
						cancelRoot()
					}
					var dependencies struct {
						DML xhandler.DML `bind:"kind=dml,required"`
					}
					if err := root.Binder.Bind(rootCtx, &dependencies); err != nil {
						return nil, err
					}
					return nil, dependencies.DML.Execute("INSERT INTO records VALUES(2)")
				}),
			})
			wantCount := 0
			if mode == "child success" {
				if err != nil {
					t.Fatalf("root completion after successful child: %v", err)
				}
				wantCount = 2
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation=%v", err)
			}
			if childStarted != (mode != "child pre-canceled") {
				t.Fatalf("child started=%v", childStarted)
			}
			var count int
			if err := h.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM records").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != wantCount {
				t.Fatalf("rows=%d want=%d", count, wantCount)
			}
		})
	}
}
