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
)

func TestQueueContractCapturedChildAfterQueueFailureCannotCommitWhenCaught(t *testing.T) {
	_ = caughtQueueHookAnchor
	for _, policy := range []string{"insert-delete", ""} {
		for _, mode := range []string{"error", "panic", "cancel"} {
			t.Run(policy+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES('insert');END"); err != nil {
					t.Fatal(err)
				}
				c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[policyReplayInput]().PkgPath(), Name: "CaughtChild"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/caught-child"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: policy, QueueContract: "source-row", EntityHooks: "caughtQueueHooks", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
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
				if parentErr == nil || rows != 0 || audit != 0 || commits != 0 {
					t.Fatalf("caught queue-contract child committed: parent=%v rows=%d audit=%d commits=%d", parentErr, rows, audit, commits)
				}
			})
		}
	}
}

func TestQueueContractEngineDispatchedNativeCallerStatementPrefix(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT UNIQUE)", "CREATE TABLE audit(action TEXT)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES('insert');END"); err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "QueueNative"}, Settings: &spec.Settings{Mutation: "post"}, Routes: []*spec.Route{{Method: "POST", Path: "/queue-native"}}, RootView: &spec.View{Name: "Rows", QueueContract: "source-row", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
	compiled, err := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[policyReplayInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/queue-native"})
	native, err := writer.New(c, reflect.TypeFor[policyReplayInput](), reflect.TypeFor[policyReplayOutput](), "post")
	if err != nil {
		t.Fatal(err)
	}
	one, two := 1, 2
	name := "dup"
	other := name
	input := &policyReplayInput{Rows: []*policyReplayRow{{ID: &one, Name: &name, Has: &policyReplayHas{ID: true, Name: true}}, {ID: &two, Name: &other, Has: &policyReplayHas{ID: true, Name: true}}}}
	tx, err := db.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = engine.New().Execute(ctx, engine.Request{Input: route, BoundInput: input, Handler: native, DataSource: dml.Source{DB: db.DB, Tx: tx}})
	if err == nil {
		t.Fatal("genuine second UNIQUE failure required")
	}
	for _, table := range []string{"records", "audit"} {
		var n int
		if err := tx.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != 1 {
			t.Fatal(table, n, err)
		}
	}
	if _, err := tx.Exec("INSERT INTO records VALUES(3,'caller-usable')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"records", "audit"} {
		var n int
		if err := db.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatal(table, n, err)
		}
	}
}
