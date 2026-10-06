package engine_test

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type deferredCapturedWriter struct {
	*writer.Handler
	issued int
}

func (h *deferredCapturedWriter) RequiresPreBindingTransaction() bool { return false }
func (h *deferredCapturedWriter) CapturedExecutionGuard(i rhandler.Invocation) (func(context.Context) error, error) {
	c, e := h.Handler.CapturedExecutionGuard(i)
	if c != nil {
		h.issued++
	}
	return c, e
}

type opaqueIdentitySource struct {
	source dml.Source
	opened int
}

func (s *opaqueIdentitySource) Open(ctx context.Context) (xhandler.Data, error) {
	s.opened++
	return s.source.Open(ctx)
}
func TestCapturedFailureBeforeOwnerEnrollmentCannotCommit(t *testing.T) {
	for _, guarded := range []bool{true, false} {
		t.Run(map[bool]string{true: "captured", false: "ordinary"}[guarded], func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if e := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "CREATE TABLE audit(id INTEGER)", "CREATE TRIGGER inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES(NEW.id);END"); e != nil {
				t.Fatal(e)
			}
			c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DeferredChild"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/deferred-child"}}, RootView: &spec.View{Name: "Rows", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "name", Source: "name"}}}}
			compiled, e := compiler.New(compiler.Input{Component: c, InputType: reflect.TypeFor[policyReplayInput]()}).Compile()
			if e != nil {
				t.Fatal(e)
			}
			childRoute, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/deferred-child"})
			native, e := writer.New(c, reflect.TypeFor[policyReplayInput](), reflect.TypeFor[policyReplayOutput](), "patch")
			if e != nil {
				t.Fatal(e)
			}
			decorated := &deferredCapturedWriter{Handler: native}
			var childHandler rhandler.Handler = decorated
			if !guarded {
				childHandler = rhandler.HandlerFunc(func(ctx context.Context, in rhandler.Invocation) (any, error) {
					_, _, e := in.Binder.Lookup(ctx, xhandler.DMLKey)
					return nil, e
				})
			}
			pc := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DeferredParent"}, Routes: []*spec.Route{{Method: "POST", Path: "/deferred-parent"}}}
			p, e := compiler.New(compiler.Input{Component: pc, InputType: reflect.TypeFor[struct{}]()}).Compile()
			if e != nil {
				t.Fatal(e)
			}
			parentRoute, _ := p.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/deferred-parent"})
			commits := 0
			source := dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}
			opaque := &opaqueIdentitySource{source: source}
			var childErr error
			_, parentErr := engine.New().Execute(ctx, engine.Request{Input: parentRoute, BoundInput: &struct{}{}, DataSource: source, Handler: rhandler.HandlerFunc(func(ctx context.Context, i rhandler.Invocation) (any, error) {
				cap, found, e := i.Binder.Lookup(ctx, xhandler.DMLKey)
				if e != nil {
					return nil, e
				}
				if !found {
					t.Fatal("no DML")
				}
				if e = cap.(xhandler.DML).Execute("INSERT INTO records VALUES(99,'parent')"); e != nil {
					return nil, e
				}
				_, childErr = engine.New().Execute(ctx, engine.Request{Input: childRoute, BoundInput: &policyReplayInput{}, DataSource: opaque, Handler: childHandler})
				return "caught", nil
			})})
			expectedIssued := 0
			if guarded {
				expectedIssued = 1
			}
			if decorated.issued != expectedIssued || !errors.Is(childErr, engine.ErrUnknownDatabaseIdentity) || opaque.opened != 0 {
				t.Fatalf("wrong path issued=%d child=%v opened=%d", decorated.issued, childErr, opaque.opened)
			}
			var rows, audit int
			if e = db.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&rows); e != nil {
				t.Fatal(e)
			}
			if e = db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&audit); e != nil {
				t.Fatal(e)
			}
			if guarded && (parentErr == nil || rows != 0 || audit != 0 || commits != 0) {
				t.Fatalf("latched captured failure lost: parent=%v child=%v issued=%d rows=%d audit=%d commits=%d", parentErr, childErr, decorated.issued, rows, audit, commits)
			}
			if !guarded && (parentErr != nil || rows != 1 || audit != 1 || commits != 1) {
				t.Fatalf("ordinary caught resolution error changed: parent=%v rows=%d audit=%d commits=%d", parentErr, rows, audit, commits)
			}
		})
	}
}
