package engine_test

import (
	"context"
	"errors"
	dexec "github.com/viant/datly/exec"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

var caughtQueueCause = errors.New("child after queue failed")
var caughtQueueHookAnchor = reflect.TypeFor[caughtQueueHooks]()

type caughtQueueHooks struct{}

func (*caughtQueueHooks) AfterQueue(ctx context.Context, row *policyReplayRow, _ xhandler.LifecycleContext[policyReplayRow, xhandler.NoParent, policyReplayOutput]) error {
	if row.Name != nil && *row.Name == "panic" {
		panic("child after queue panic")
	}
	if row.Name != nil && *row.Name == "cancel" {
		return context.Canceled
	}
	return caughtQueueCause
}
func TestCapturedChildAfterQueueFailureCannotCommitWhenCaught(t *testing.T) {
	_ = caughtQueueHookAnchor
	for _, policy := range []string{"insert-delete", ""} {
		for _, mode := range []string{"error", "panic", "cancel"} {
			t.Run(policy+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES('insert');END"); err != nil {
					t.Fatal(err)
				}
				c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[policyReplayInput]().PkgPath(), Name: "CaughtChild"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/caught-child"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: policy, EntityHooks: "caughtQueueHooks", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
				compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[policyReplayInput]()}).Compile()
				if err != nil {
					t.Fatal(err)
				}
				childRoute, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/caught-child"})
				if !ok {
					t.Fatal("child route missing")
				}
				h, err := writer.New(c, reflect.TypeFor[policyReplayInput](), reflect.TypeFor[policyReplayOutput](), "patch")
				if err != nil {
					t.Fatal(err)
				}
				parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "CaughtParent"}, Routes: []*spec.Route{{Method: "POST", Path: "/caught-parent"}}}
				pc, err := compiler.New(compiler.Input{Component: parent, InputType: reflect.TypeFor[struct{}]()}).Compile()
				if err != nil {
					t.Fatal(err)
				}
				parentRoute, _ := pc.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/caught-parent"})
				id := 1
				name := mode
				input := &policyReplayInput{Rows: []*policyReplayRow{{ID: &id, Name: &name, Has: &policyReplayHas{ID: true, Name: true}}}}
				commits := 0
				source := dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}
				var childErr error
				_, parentErr := engine.New().Execute(ctx, engine.Request{Input: parentRoute, BoundInput: &struct{}{}, DataSource: source, Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
					_, childErr = engine.New().Execute(ctx, engine.Request{Input: childRoute, BoundInput: input, Handler: h, DataSource: source})
					return "parent caught child error", nil
				})})
				var panicErr *dexec.PanicError
				matched := errors.Is(childErr, caughtQueueCause)
				if mode == "cancel" {
					matched = errors.Is(childErr, context.Canceled)
				}
				if mode == "panic" {
					matched = errors.As(childErr, &panicErr) && panicErr.Cause() == "child after queue panic"
				}
				if !matched {
					t.Fatalf("hook error not reached: %v", childErr)
				}
				var rows, audit int
				if err = db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); err != nil {
					t.Fatal(err)
				}
				if err = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audit); err != nil {
					t.Fatal(err)
				}
				if policy != "" {
					if parentErr == nil || rows != 0 || audit != 0 || commits != 0 {
						t.Fatalf("caught protected child committed: parent=%v rows=%d audit=%d commits=%d", parentErr, rows, audit, commits)
					}
				} else {
					if rows != 1 || audit != 1 || commits != 1 {
						t.Fatalf("ordinary caught-child behavior changed: parent=%v rows=%d audit=%d commits=%d", parentErr, rows, audit, commits)
					}
				}
			})
		}
	}
}
