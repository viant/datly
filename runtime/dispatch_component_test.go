package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type componentChildInput struct {
	Name string
}

type componentChildOutput struct {
	Message string
}

type componentParentInput struct {
	Child *componentChildOutput
}

type componentParentOutput struct {
	Message string
}

func TestRuntimeBindsComponentOutputThroughUnifiedEngine(t *testing.T) {
	child := componentSpec("Child", http.MethodGet, "/child", []*spec.Parameter{{
		Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}, TypeExpr: "string",
	}})
	parent := componentSpec("Parent", http.MethodGet, "/parent", []*spec.Parameter{{
		Name: "Child", Source: spec.BindSource{Kind: "component", Name: "GET:/child"}, TypeExpr: "*componentChildOutput",
	}})
	childArtifact := componentArtifact(t, child, reflect.TypeOf(componentChildInput{}), reflect.TypeOf(componentChildOutput{}))
	parentArtifact := componentArtifact(t, parent, reflect.TypeOf(componentParentInput{}), reflect.TypeOf(componentParentOutput{}))
	runtime, err := NewRuntime([]*registry.RegisteredComponent{
		{
			Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeOf(componentChildOutput{}),
			Handler: customhandler.NewFunc[componentChildInput, componentChildOutput](func(_ context.Context, input *componentChildInput) (*componentChildOutput, error) {
				return &componentChildOutput{Message: "hello " + input.Name}, nil
			}),
		},
		{
			Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeOf(componentParentOutput{}),
			Handler: customhandler.NewFunc[componentParentInput, componentParentOutput](func(_ context.Context, input *componentParentInput) (*componentParentOutput, error) {
				if input.Child == nil {
					return nil, errors.New("child output was not injected")
				}
				return &componentParentOutput{Message: input.Child.Message}, nil
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := executeTestRoute(t, runtime, context.Background(), testharness.NewRequest(http.MethodGet, "/parent").WithQuery(url.Values{"name": {"Ada"}}))
	if err != nil || actual.(*componentParentOutput).Message != "hello Ada" {
		t.Fatalf("ExecuteRoute() = (%+v, %v)", actual, err)
	}
}

func TestRuntimeRejectsComponentDependencyCycle(t *testing.T) {
	type leftInput struct{ Right *componentChildOutput }
	type rightInput struct{ Left *componentChildOutput }
	left := componentSpec("Left", http.MethodGet, "/left", []*spec.Parameter{{Name: "Right", Source: spec.BindSource{Kind: "component", Name: "GET:/right"}}})
	right := componentSpec("Right", http.MethodGet, "/right", []*spec.Parameter{{Name: "Left", Source: spec.BindSource{Kind: "component", Name: "GET:/left"}}})
	leftArtifact := componentArtifact(t, left, reflect.TypeOf(leftInput{}), reflect.TypeOf(componentChildOutput{}))
	rightArtifact := componentArtifact(t, right, reflect.TypeOf(rightInput{}), reflect.TypeOf(componentChildOutput{}))
	runtime, err := NewRuntime([]*registry.RegisteredComponent{
		{Component: leftArtifact.Component, Input: leftArtifact.Input, OutputType: reflect.TypeOf(componentChildOutput{}), Handler: customhandler.NewFunc[leftInput, componentChildOutput](nil)},
		{Component: rightArtifact.Component, Input: rightArtifact.Input, OutputType: reflect.TypeOf(componentChildOutput{}), Handler: customhandler.NewFunc[rightInput, componentChildOutput](nil)},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.ExecuteRoute(context.Background(), http.MethodGet, "/left", nil)
	if err == nil || !strings.Contains(err.Error(), "component dependency cycle") {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
}

func TestRuntimeCycleDetectionUsesExactComponentRouteIdentity(t *testing.T) {
	type input struct {
		Mode string
	}
	type output struct{}
	component := componentSpec("MultiRoute", http.MethodGet, "/multi/a", []*spec.Parameter{{
		Name: "Mode", Source: spec.BindSource{Kind: "mode", Name: "mode"}, TypeExpr: "string",
	}})
	component.Routes = append(component.Routes, &spec.Route{Method: http.MethodGet, Path: "/multi/b"})
	artifact := componentArtifact(t, component, reflect.TypeOf(input{}), reflect.TypeOf(output{}))
	targetA := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/multi/a"}}
	targetB := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/multi/b"}}
	var invoked []string
	handler := customhandler.New[input, output](xhandler.ContractFunc[input, output](func(ctx context.Context, session xhandler.Session, input *input, _ *output) error {
		invoked = append(invoked, input.Mode)
		var target dexec.ComponentTarget
		var mode string
		switch input.Mode {
		case "AtoB":
			target, mode = targetB, "B"
		case "AtoA":
			target, mode = targetA, "AtoA"
		case "AtoBtoA":
			target, mode = targetB, "BtoA"
		case "BtoA":
			target, mode = targetA, "done"
		default:
			return nil
		}
		value, found, err := session.Binder().Lookup(ctx, dexec.ComponentInvokerKey)
		if err != nil || !found {
			return fmt.Errorf("component invoker lookup: found=%v err=%w", found, err)
		}
		invoker, ok := value.(dexec.ComponentInvoker)
		if !ok {
			return fmt.Errorf("component invoker has type %T", value)
		}
		_, err = invoker.InvokeComponent(ctx, dexec.ComponentRequest{
			Target: target,
			Providers: []locator.Provider{
				namedProvider("mode", "mode", mode),
			},
		})
		return err
	}))
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Handler: handler,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name    string
		mode    string
		want    []string
		wantErr bool
	}{
		{name: "different route is allowed", mode: "AtoB", want: []string{"AtoB", "B"}},
		{name: "same route recursion is rejected", mode: "AtoA", want: []string{"AtoA"}, wantErr: true},
		{name: "cross route recursion is rejected", mode: "AtoBtoA", want: []string{"AtoBtoA", "BtoA"}, wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			invoked = nil
			_, invokeErr := runtime.InvokeComponent(context.Background(), dexec.ComponentRequest{
				Target: targetA,
				Providers: []locator.Provider{
					namedProvider("mode", "mode", testCase.mode),
				},
			})
			if (invokeErr != nil) != testCase.wantErr {
				t.Fatalf("InvokeComponent() error = %v, wantErr=%v", invokeErr, testCase.wantErr)
			}
			if testCase.wantErr && !strings.Contains(invokeErr.Error(), "component dependency cycle") {
				t.Fatalf("InvokeComponent() error = %v", invokeErr)
			}
			if !reflect.DeepEqual(invoked, testCase.want) {
				t.Fatalf("handler invocations = %v, want %v", invoked, testCase.want)
			}
		})
	}
}

type exactInvocationInput struct {
	Name        string
	Initialized int
}

func (i *exactInvocationInput) Init(context.Context) error {
	i.Initialized++
	return nil
}

type exactInvocationOutput struct {
	Name        string
	Initialized int
}

func TestRuntimeRouteAndExactTargetShareLifecycle(t *testing.T) {
	component := componentSpec("Exact", http.MethodGet, "/exact", []*spec.Parameter{{
		Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}, TypeExpr: "string",
	}})
	artifact := componentArtifact(t, component, reflect.TypeOf(exactInvocationInput{}), reflect.TypeOf(exactInvocationOutput{}))
	var inputs []*exactInvocationInput
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{
		Component:  artifact.Component,
		Input:      artifact.Input,
		OutputType: reflect.TypeOf(exactInvocationOutput{}),
		Handler: customhandler.NewFunc[exactInvocationInput, exactInvocationOutput](func(_ context.Context, input *exactInvocationInput) (*exactInvocationOutput, error) {
			inputs = append(inputs, input)
			return &exactInvocationOutput{Name: input.Name, Initialized: input.Initialized}, nil
		}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	provider := func(value string) locator.Provider {
		return namedProvider("query", "name", value)
	}
	routeActual, err := runtime.ExecuteRoute(context.Background(), http.MethodGet, "/exact", runtimeProviderScope{provider("route")})
	if err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	exactActual, err := runtime.InvokeComponent(context.Background(), dexec.ComponentRequest{
		Target:    dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/exact"}},
		Providers: []locator.Provider{provider("exact")},
	})
	if err != nil {
		t.Fatalf("InvokeComponent() error = %v", err)
	}
	bound := &exactInvocationInput{Name: "bound"}
	boundActual, err := runtime.InvokeComponent(context.Background(), dexec.ComponentRequest{
		Target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/exact"}},
		Input:  bound,
	})
	if err != nil {
		t.Fatalf("InvokeComponent(bound) error = %v", err)
	}
	outputs := []*exactInvocationOutput{
		routeActual.(*exactInvocationOutput), exactActual.(*exactInvocationOutput), boundActual.(*exactInvocationOutput),
	}
	if !reflect.DeepEqual(outputs, []*exactInvocationOutput{
		{Name: "route", Initialized: 1}, {Name: "exact", Initialized: 1}, {Name: "bound", Initialized: 1},
	}) {
		t.Fatalf("outputs = %+v", outputs)
	}
	if len(inputs) != 3 || inputs[2] != bound {
		t.Fatalf("handler inputs = %#v", inputs)
	}
}

type componentLifecycleInput struct {
	Trace *[]string
}

func (i *componentLifecycleInput) Init(context.Context) error {
	*i.Trace = append(*i.Trace, "init")
	return nil
}

func (i *componentLifecycleInput) InitMCP(context.Context, xmcp.Context) error {
	*i.Trace = append(*i.Trace, "init_mcp")
	return nil
}

type componentLifecycleOutput struct {
	Trace *[]string
}

func (o *componentLifecycleOutput) Finalize(context.Context) error {
	*o.Trace = append(*o.Trace, "finalize")
	return nil
}

func (o *componentLifecycleOutput) FinalizeMCP(context.Context, xmcp.Context) error {
	*o.Trace = append(*o.Trace, "finalize_mcp")
	return nil
}

type runtimeMCPContext struct{}

func (runtimeMCPContext) Client() xmcp.Client { return nil }

func TestRuntimeInvocationModesShareCompleteLifecycle(t *testing.T) {
	type parentInput struct{}
	type parentOutput struct{}
	child := componentSpec("LifecycleChild", http.MethodPost, "/lifecycle-child", []*spec.Parameter{{
		Name: "Trace", Source: spec.BindSource{Kind: "trace", Name: "trace"},
	}})
	parent := componentSpec("LifecycleParent", http.MethodPost, "/lifecycle-parent", nil)
	childArtifact := componentArtifact(t, child, reflect.TypeOf(componentLifecycleInput{}), reflect.TypeOf(componentLifecycleOutput{}))
	parentArtifact := componentArtifact(t, parent, reflect.TypeOf(parentInput{}), reflect.TypeOf(parentOutput{}))
	childTarget := dexec.ComponentTarget{Component: child.Key, Route: spec.RouteRef{Method: http.MethodPost, Path: "/lifecycle-child"}}
	nestedLookups := 0
	childHandler := customhandler.NewFunc[componentLifecycleInput, componentLifecycleOutput](func(_ context.Context, input *componentLifecycleInput) (*componentLifecycleOutput, error) {
		*input.Trace = append(*input.Trace, "handler")
		return &componentLifecycleOutput{Trace: input.Trace}, nil
	})
	parentHandler := customhandler.New[parentInput, parentOutput](xhandler.ContractFunc[parentInput, parentOutput](func(ctx context.Context, session xhandler.Session, _ *parentInput, _ *parentOutput) error {
		value, found, err := session.Binder().Lookup(ctx, dexec.ComponentInvokerKey)
		if err != nil || !found {
			return fmt.Errorf("component invoker lookup: found=%v err=%w", found, err)
		}
		invoker, ok := value.(dexec.ComponentInvoker)
		if !ok {
			return fmt.Errorf("component invoker has type %T", value)
		}
		trace, found, err := session.Binder().Lookup(ctx, xhandler.ValueKey("nested_trace"))
		if err != nil || !found {
			return fmt.Errorf("nested trace lookup: found=%v err=%w", found, err)
		}
		_, err = invoker.InvokeComponent(ctx, dexec.ComponentRequest{
			Target: childTarget,
			Providers: []locator.Provider{
				handlerprovider.Named("trace", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
					if name != "trace" {
						return nil, false, nil
					}
					nestedLookups++
					return trace, true, nil
				}),
			},
		})
		return err
	}))
	runtime, err := NewRuntime([]*registry.RegisteredComponent{
		{Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeOf(componentLifecycleOutput{}), Handler: childHandler},
		{Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeOf(parentOutput{}), Handler: parentHandler},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := xmcp.WithContext(context.Background(), runtimeMCPContext{})
	want := []string{"init", "init_mcp", "handler", "finalize", "finalize_mcp"}
	assertTrace := func(name string, trace []string, invokeErr error) {
		t.Helper()
		if invokeErr != nil || !reflect.DeepEqual(trace, want) {
			t.Fatalf("%s lifecycle = (%v, %v), want %v", name, trace, invokeErr, want)
		}
	}

	var routeTrace []string
	_, err = runtime.ExecuteRoute(ctx, http.MethodPost, "/lifecycle-child", runtimeProviderScope{
		namedProvider("trace", "trace", &routeTrace),
	})
	assertTrace("route", routeTrace, err)

	var exactTrace []string
	_, err = runtime.InvokeComponent(ctx, dexec.ComponentRequest{
		Target: childTarget,
		Providers: []locator.Provider{
			namedProvider("trace", "trace", &exactTrace),
		},
	})
	assertTrace("exact provider", exactTrace, err)

	var boundTrace []string
	_, err = runtime.InvokeComponent(ctx, dexec.ComponentRequest{
		Target: childTarget,
		Input:  &componentLifecycleInput{Trace: &boundTrace},
	})
	assertTrace("exact bound", boundTrace, err)

	var nestedTrace []string
	_, err = runtime.ExecuteRoute(ctx, http.MethodPost, "/lifecycle-parent", runtimeProviderScope{
		handlerprovider.Static(xhandler.ValueKey("nested_trace"), &nestedTrace),
	})
	assertTrace("nested", nestedTrace, err)
	if nestedLookups != 1 {
		t.Fatalf("nested child input provider lookups = %d, want 1", nestedLookups)
	}
}

func TestRuntimeExactInvocationRejectsInvalidAuthorityBeforeHandler(t *testing.T) {
	type input struct{}
	type output struct{}
	primary := componentSpec("ExactAuthority", http.MethodGet, "/exact-authority", nil)
	other := componentSpec("OtherAuthority", http.MethodGet, "/other-authority", nil)
	primaryArtifact := componentArtifact(t, primary, reflect.TypeOf(input{}), reflect.TypeOf(output{}))
	otherArtifact := componentArtifact(t, other, reflect.TypeOf(input{}), reflect.TypeOf(output{}))
	invocations := 0
	handler := customhandler.NewFunc[input, output](func(context.Context, *input) (*output, error) {
		invocations++
		return &output{}, nil
	})
	runtime, err := NewRuntime([]*registry.RegisteredComponent{
		{Component: primaryArtifact.Component, Input: primaryArtifact.Input, OutputType: reflect.TypeOf(output{}), Handler: handler},
		{Component: otherArtifact.Component, Input: otherArtifact.Input, OutputType: reflect.TypeOf(output{}), Handler: handler},
	})
	if err != nil {
		t.Fatal(err)
	}
	validTarget := dexec.ComponentTarget{Component: primary.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/exact-authority"}}
	tests := []struct {
		name    string
		request dexec.ComponentRequest
		want    string
	}{
		{name: "wrong key kind", request: dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindView, Name: primary.Name}, Route: validTarget.Route}}, want: "requires a component key"},
		{name: "missing key name", request: dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent}, Route: validTarget.Route}}, want: "requires a component key"},
		{name: "unregistered component", request: dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: primary.Key.Scope, Name: "Missing"}, Route: validTarget.Route}}, want: "registered component not found"},
		{name: "invalid route", request: dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: primary.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "relative"}}}, want: "must use METHOD:/path"},
		{name: "unowned route", request: dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: primary.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/other-authority"}}}, want: "does not own route"},
		{name: "wrong bound input", request: dexec.ComponentRequest{Target: validTarget, Input: &struct{}{}}, want: "bound component input"},
		{name: "protected child provider", request: dexec.ComponentRequest{Target: validTarget, Providers: []locator.Provider{handlerprovider.Static(xhandler.InputKey, &input{})}}, want: "protected runtime kind"},
		{name: "duplicate child provider", request: dexec.ComponentRequest{Target: validTarget, Providers: []locator.Provider{namedProvider("query", "a", "one"), namedProvider("query", "b", "two")}}, want: "provider kind \"query\" is duplicated"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			before := invocations
			_, invokeErr := runtime.InvokeComponent(context.Background(), testCase.request)
			if invokeErr == nil || !strings.Contains(invokeErr.Error(), testCase.want) {
				t.Fatalf("InvokeComponent() error = %v, want %q", invokeErr, testCase.want)
			}
			if invocations != before {
				t.Fatalf("handler invoked for rejected request: before=%d after=%d", before, invocations)
			}
		})
	}
}

func TestRuntimeInjectsScopedExactComponentInvoker(t *testing.T) {
	type childInput struct{ Name string }
	type childOutput struct{ Name string }
	type parentInput struct{}
	type parentOutput struct{ Name string }
	child := componentSpec("ExactChild", http.MethodGet, "/exact-child", []*spec.Parameter{{
		Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}, TypeExpr: "string",
	}})
	parent := componentSpec("ExactParent", http.MethodGet, "/exact-parent", nil)
	childArtifact := componentArtifact(t, child, reflect.TypeOf(childInput{}), reflect.TypeOf(childOutput{}))
	parentArtifact := componentArtifact(t, parent, reflect.TypeOf(parentInput{}), reflect.TypeOf(parentOutput{}))
	parentContract := xhandler.ContractFunc[parentInput, parentOutput](func(ctx context.Context, session xhandler.Session, _ *parentInput, output *parentOutput) error {
		value, found, err := session.Binder().Lookup(ctx, dexec.ComponentInvokerKey)
		if err != nil || !found {
			return fmt.Errorf("component invoker lookup: found=%v err=%w", found, err)
		}
		invoker, ok := value.(dexec.ComponentInvoker)
		if !ok {
			return fmt.Errorf("component invoker has type %T", value)
		}
		actual, err := invoker.InvokeComponent(ctx, dexec.ComponentRequest{
			Target:    dexec.ComponentTarget{Component: child.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/exact-child"}},
			Providers: []locator.Provider{namedProvider("query", "name", "child")},
		})
		if err != nil {
			return err
		}
		output.Name = actual.(*childOutput).Name
		return nil
	})
	runtime, err := NewRuntime([]*registry.RegisteredComponent{
		{
			Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeOf(childOutput{}),
			Handler: customhandler.NewFunc[childInput, childOutput](func(_ context.Context, input *childInput) (*childOutput, error) {
				return &childOutput{Name: input.Name}, nil
			}),
		},
		{
			Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeOf(parentOutput{}),
			Handler: customhandler.New[parentInput, parentOutput](parentContract),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := runtime.ExecuteRoute(context.Background(), http.MethodGet, "/exact-parent", runtimeProviderScope{
		namedProvider("query", "name", "inherited"),
	})
	if err != nil || actual.(*parentOutput).Name != "child" {
		t.Fatalf("ExecuteRoute() = (%+v, %v)", actual, err)
	}
}

func namedProvider(kind, name string, value any) locator.Provider {
	return handlerprovider.Named(kind, func(_ context.Context, _ reflect.Type, requested string) (any, bool, error) {
		if requested != name {
			return nil, false, nil
		}
		return value, true, nil
	})
}

type componentDataSource struct {
	data  xhandler.Data
	opens int
	key   any
}

func (s *componentDataSource) Open(context.Context) (xhandler.Data, error) {
	s.opens++
	return s.data, nil
}

func (s *componentDataSource) InvocationKey() any { return s.key }

func TestRuntimeMultiComponentTransactionOwnership(t *testing.T) {
	tests := []struct {
		name              string
		parentTransaction bool
		commitParent      bool
		flushChild        bool
		failParent        bool
		wantFlushError    bool
		wantPending       int
		wantRows          int
	}{
		{name: "parent owner commits", parentTransaction: true, commitParent: true, wantPending: 2, wantRows: 2},
		{name: "parent owner rolls back", parentTransaction: true, wantPending: 2, wantRows: 0},
		{name: "binding flush is rejected with parent owner", parentTransaction: true, flushChild: true, wantFlushError: true},
		{name: "root flush manages local transaction", wantRows: 2},
		{name: "binding flush is rejected with local owner", flushChild: true, wantFlushError: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			harness := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := harness.ExecStatements(ctx, `CREATE TABLE audit (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
				t.Fatal(err)
			}
			type row struct {
				ID   int    `sqlx:"id,primaryKey"`
				Name string `sqlx:"name"`
			}
			type childInput struct{}
			type childOutput struct{ ID int }
			type parentInput struct{ Child *childOutput }
			type parentOutput struct{}

			child := componentSpec("ChildWrite", http.MethodPost, "/child-write", nil)
			parent := componentSpec("ParentWrite", http.MethodPost, "/parent-write", []*spec.Parameter{{
				Name: "Child", Source: spec.BindSource{Kind: "component", Name: "POST:/child-write"},
			}})
			childArtifact := componentArtifact(t, child, reflect.TypeOf(childInput{}), reflect.TypeOf(childOutput{}))
			parentArtifact := componentArtifact(t, parent, reflect.TypeOf(parentInput{}), reflect.TypeOf(parentOutput{}))

			var parentTx *sql.Tx
			var rootData xhandler.Data
			if testCase.parentTransaction {
				var err error
				parentTx, err = harness.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				rootData = dml.NewData(harness.DB, dml.WithTx(parentTx))
			} else {
				rootData = dml.NewData(harness.DB)
			}
			rootSource := &componentDataSource{data: rootData, key: harness.DB}
			childSource := &componentDataSource{data: dml.NewData(harness.DB), key: harness.DB}
			expected := errors.New("parent failed")

			childContract := xhandler.ContractFunc[childInput, childOutput](func(ctx context.Context, session xhandler.Session, _ *childInput, output *childOutput) error {
				resolved, found, err := session.Binder().Lookup(ctx, xhandler.DataKey)
				if err != nil || !found {
					return errors.New("child data capability missing")
				}
				data := resolved.(xhandler.Data)
				output.ID = 1
				if err = data.Insert("audit", &row{ID: 1, Name: "child"}); err != nil {
					return err
				}
				if testCase.flushChild {
					return data.Flush(ctx, "audit")
				}
				return nil
			})
			parentContract := xhandler.ContractFunc[parentInput, parentOutput](func(ctx context.Context, session xhandler.Session, input *parentInput, _ *parentOutput) error {
				if input.Child == nil || input.Child.ID != 1 {
					return errors.New("child output missing")
				}
				resolved, found, err := session.Binder().Lookup(ctx, xhandler.DMLKey)
				if err != nil || !found {
					return errors.New("parent DML capability missing")
				}
				if err = resolved.(xhandler.DML).Insert("audit", &row{ID: 2, Name: "parent"}); err != nil {
					return err
				}
				if testCase.failParent {
					return expected
				}
				return nil
			})
			runtime, err := NewRuntime([]*registry.RegisteredComponent{
				{
					Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeOf(childOutput{}),
					Handler: customhandler.New[childInput, childOutput](childContract), DataSource: childSource,
				},
				{
					Component: parentArtifact.Component, Input: parentArtifact.Input, OutputType: reflect.TypeOf(parentOutput{}),
					Handler: customhandler.New[parentInput, parentOutput](parentContract), DataSource: rootSource,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = runtime.ExecuteRoute(ctx, http.MethodPost, "/parent-write", nil)
			if testCase.wantFlushError {
				if err == nil || !strings.Contains(err.Error(), "cannot flush a binding component") {
					t.Fatalf("ExecuteRoute() error = %v, want binding flush rejection", err)
				}
			} else if testCase.failParent {
				if !errors.Is(err, expected) {
					t.Fatalf("ExecuteRoute() error = %v", err)
				}
			} else if err != nil {
				t.Fatalf("ExecuteRoute() error = %v", err)
			}
			if rootSource.opens != 1 || childSource.opens != 0 {
				t.Fatalf("data source opens: root=%d child=%d", rootSource.opens, childSource.opens)
			}

			if parentTx != nil {
				var pending int
				if err = parentTx.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&pending); err != nil || pending != testCase.wantPending {
					t.Fatalf("parent transaction row count = %d, want %d, err = %v", pending, testCase.wantPending, err)
				}
				if testCase.commitParent {
					err = parentTx.Commit()
				} else {
					err = parentTx.Rollback()
				}
				if err != nil {
					t.Fatalf("complete parent transaction: %v", err)
				}
			}
			var count int
			if err = harness.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit`).Scan(&count); err != nil || count != testCase.wantRows {
				t.Fatalf("committed row count = %d, want %d, err = %v", count, testCase.wantRows, err)
			}
		})
	}
}

func componentSpec(name, method, path string, params []*spec.Parameter) *spec.Component {
	return &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/components", Name: name}, Name: name,
		Routes: []*spec.Route{{Method: method, Path: path}}, Parameters: params,
	}
}

func componentArtifact(t *testing.T, component *spec.Component, inputType, outputType reflect.Type) *bootstrap.Artifact {
	t.Helper()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: outputType})
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}
