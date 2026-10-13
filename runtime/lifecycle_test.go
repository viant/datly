package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type invokerLifecycleInput struct {
	Failure     string `parameter:"Failure,kind=query,in=failure"`
	initialized bool
}

func (i *invokerLifecycleInput) Init(ctx context.Context, invoker xhandler.ComponentInvoker) error {
	_, err := invoker.Invoke(ctx, lifecycleChildRequest(1, i.Failure == "init"))
	i.initialized = err == nil
	return err
}

type invokerLifecycleOutput struct {
	failure  string
	retained xhandler.ComponentInvoker
	calls    int
}

func (o *invokerLifecycleOutput) Finalize(ctx context.Context, invoker xhandler.ComponentInvoker) error {
	o.calls++
	o.retained = invoker
	_, err := invoker.Invoke(context.Background(), lifecycleChildRequest(3, o.failure == "child" || o.failure == "swallowed"))
	if o.failure == "swallowed" {
		return nil
	}
	if err != nil {
		return err
	}
	if o.failure == "finalize" {
		return errLifecycleFinalizer
	}
	if o.failure == "panic" {
		panic("private finalizer panic")
	}
	return nil
}

type invokerLifecycleChildInput struct {
	ID   int
	Fail bool
}
type invokerLifecycleChildOutput struct{}

var errLifecycleChild = errors.New("lifecycle child failed")
var errLifecycleFinalizer = errors.New("lifecycle finalizer failed")
var errLifecycleHandler = errors.New("lifecycle handler failed")

func lifecycleChildRequest(id int, fail bool) xhandler.ComponentRequest {
	return xhandler.ComponentRequest{
		Target: xhandler.ComponentTarget{Scope: "example.com/components", Name: "LifecycleChild", Method: "POST", Path: "/lifecycle/child"},
		Input:  &invokerLifecycleChildInput{ID: id, Fail: fail},
	}
}

func TestInvokerLifecycleTransactionSQLite(t *testing.T) {
	for _, sourceLess := range []bool{false, true} {
		for _, failure := range []string{"", "init", "handler", "child", "swallowed", "finalize", "panic"} {
			t.Run(fmt.Sprintf("sourceLess=%t/failure=%s", sourceLess, failure), func(t *testing.T) {
				h := testharness.NewSQLiteHarness(t)
				if err := h.ExecStatements(t.Context(), "CREATE TABLE audit(id INTEGER PRIMARY KEY)"); err != nil {
					t.Fatal(err)
				}
				commits, executes := 0, 0
				output := &invokerLifecycleOutput{failure: failure}
				parentSpec := componentSpec("LifecycleParent", "POST", "/lifecycle/parent", nil)
				parentArtifact := componentArtifact(t, parentSpec, reflect.TypeFor[invokerLifecycleInput](), reflect.TypeFor[invokerLifecycleOutput]())
				parent := &registry.RegisteredComponent{Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeFor[invokerLifecycleOutput](),
					Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
						executes++
						if !invocation.Input.(*invokerLifecycleInput).initialized || commits != 0 {
							t.Fatal("initialization did not complete inside root transaction")
						}
						if !sourceLess {
							value, found, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
							if err != nil || !found {
								return nil, fmt.Errorf("parent DML: %v", err)
							}
							if err = value.(xhandler.DML).Execute("INSERT INTO audit VALUES(2)"); err != nil {
								return nil, err
							}
						}
						if failure == "handler" {
							return output, errLifecycleHandler
						}
						return output, nil
					}),
				}
				source := dml.Source{DB: h.DB, OnCommit: func(context.Context) { commits++ }}
				if !sourceLess {
					parent.DataSource = source
				}
				childSpec := componentSpec("LifecycleChild", "POST", "/lifecycle/child", nil)
				childArtifact := componentArtifact(t, childSpec, reflect.TypeFor[invokerLifecycleChildInput](), reflect.TypeFor[invokerLifecycleChildOutput]())
				child := &registry.RegisteredComponent{Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeFor[invokerLifecycleChildOutput](), DataSource: source,
					Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
						input := invocation.Input.(*invokerLifecycleChildInput)
						if commits != 0 {
							t.Fatal("child observed a prematurely committed root")
						}
						value, found, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
						if err != nil || !found {
							return nil, fmt.Errorf("child DML: %v", err)
						}
						if err = value.(xhandler.DML).Execute("INSERT INTO audit VALUES(?)", input.ID); err != nil {
							return nil, err
						}
						if input.Fail {
							return nil, errLifecycleChild
						}
						return &invokerLifecycleChildOutput{}, nil
					}),
				}
				rt, err := NewRuntime([]*registry.RegisteredComponent{parent, child})
				if err != nil {
					t.Fatal(err)
				}
				scope, err := requestprovider.New(httptest.NewRequest("POST", "/lifecycle/parent?failure="+failure, nil))
				if err != nil {
					t.Fatal(err)
				}
				_, err = rt.ExecuteRoute(t.Context(), "POST", "/lifecycle/parent", scope)
				if failure == "" {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("failure was swallowed")
				}
				if (failure == "init" || failure == "child" || failure == "swallowed") && !errors.Is(err, errLifecycleChild) {
					t.Fatalf("child failure lost: %v", err)
				}
				if failure == "handler" && !errors.Is(err, errLifecycleHandler) {
					t.Fatalf("handler failure lost: %v", err)
				}
				if failure == "finalize" && !errors.Is(err, errLifecycleFinalizer) {
					t.Fatalf("finalizer failure lost: %v", err)
				}
				if failure == "init" {
					if executes != 0 || output.calls != 0 {
						t.Fatal("execution/finalization ran after failed Init")
					}
				} else {
					if executes != 1 || output.calls != 1 {
						t.Fatalf("execute=%d finalize=%d", executes, output.calls)
					}
					if _, escaped := output.retained.Invoke(t.Context(), lifecycleChildRequest(4, false)); escaped == nil || !strings.Contains(escaped.Error(), "scope is closed") {
						t.Fatalf("retained invoker: %v", escaped)
					}
				}
				var count int
				if err = h.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count); err != nil {
					t.Fatal(err)
				}
				want := 0
				if failure == "" {
					want = 3
					if sourceLess {
						want = 2
					}
					if commits != 1 {
						t.Fatalf("commits=%d", commits)
					}
				} else if commits != 0 {
					t.Fatalf("committed failed lifecycle: %d", commits)
				}
				if count != want {
					t.Fatalf("rows=%d want=%d", count, want)
				}
			})
		}
	}
}

type invokerWriterInput bufferedCallInput

func (*invokerWriterInput) Init(ctx context.Context, invoker xhandler.ComponentInvoker) error {
	_, err := invoker.Invoke(ctx, lifecycleChildRequest(1, false))
	return err
}

type invokerWriterOutput bufferedCallOutput

func (*invokerWriterOutput) Finalize(ctx context.Context, invoker xhandler.ComponentInvoker) error {
	_, err := invoker.Invoke(ctx, lifecycleChildRequest(3, false))
	return err
}

func TestInvokerLifecycleNativeWriterSQLite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("childFailure=%t", fail), func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			if err := h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT NOT NULL)", "CREATE TABLE audit(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			commits, childCalls := 0, 0
			source := dml.Source{DB: h.DB, OnCommit: func(context.Context) { commits++ }}
			rootSpec := componentSpec("LifecycleWriter", "PATCH", "/lifecycle/writer", nil)
			rootSpec.Settings = &spec.Settings{Mutation: "patch"}
			rootSpec.RootView = &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "label", Source: "label"}}}
			artifact := componentArtifact(t, rootSpec, reflect.TypeFor[invokerWriterInput](), reflect.TypeFor[invokerWriterOutput]())
			native, err := writer.New(artifact.Component, reflect.TypeFor[invokerWriterInput](), reflect.TypeFor[invokerWriterOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			views, err := artifact.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			root := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[invokerWriterOutput](), Handler: native, Providers: []locator.Provider{views}, DataSource: source}
			childSpec := componentSpec("LifecycleChild", "POST", "/lifecycle/child", nil)
			childArtifact := componentArtifact(t, childSpec, reflect.TypeFor[invokerLifecycleChildInput](), reflect.TypeFor[invokerLifecycleChildOutput]())
			child := &registry.RegisteredComponent{Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeFor[invokerLifecycleChildOutput](), DataSource: source,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					childCalls++
					if commits != 0 {
						t.Fatal("child ran after commit")
					}
					input := invocation.Input.(*invokerLifecycleChildInput)
					value, found, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
					if err != nil || !found {
						return nil, fmt.Errorf("child DML: %v", err)
					}
					if err = value.(xhandler.DML).Execute("INSERT INTO audit VALUES(?)", input.ID); err != nil {
						return nil, err
					}
					if input.ID == 3 && fail {
						return nil, errLifecycleChild
					}
					return &invokerLifecycleChildOutput{}, nil
				}),
			}
			rt, err := NewRuntime([]*registry.RegisteredComponent{root, child})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("PATCH", "/lifecycle/writer", strings.NewReader(`{"data":[{"id":2,"label":"writer"}]}`))
			req.Header.Set("Content-Type", "application/json")
			scope, err := requestprovider.New(req)
			if err != nil {
				t.Fatal(err)
			}
			_, err = rt.ExecuteRoute(t.Context(), "PATCH", "/lifecycle/writer", scope)
			if fail {
				if !errors.Is(err, errLifecycleChild) {
					t.Fatalf("lost child failure: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if childCalls != 2 {
				t.Fatalf("child calls=%d", childCalls)
			}
			for _, table := range []string{"records", "audit"} {
				want := 1
				if table == "audit" {
					want = 2
				}
				if fail {
					want = 0
				}
				var count int
				if err := h.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != want {
					t.Fatalf("%s rows=%d want=%d err=%v", table, count, want, err)
				}
			}
			wantCommits := 1
			if fail {
				wantCommits = 0
			}
			if commits != wantCommits {
				t.Fatalf("commits=%d", commits)
			}
		})
	}
}
