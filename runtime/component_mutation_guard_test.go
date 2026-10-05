package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

type componentGuardInput struct{}
type componentGuardOutput struct{}
type componentGuardBindingInput struct{ Value string }
type componentGuardDependencyInput struct{ Child *componentGuardOutput }
type componentGuardData struct{ writes int }

func (d *componentGuardData) Insert(string, any) error     { d.writes++; return nil }
func (d *componentGuardData) Update(string, any) error     { d.writes++; return nil }
func (d *componentGuardData) Delete(string, any) error     { d.writes++; return nil }
func (d *componentGuardData) Execute(string, ...any) error { d.writes++; return nil }
func (d *componentGuardData) Allocate(context.Context, string, any, string) error {
	d.writes++
	return nil
}
func (d *componentGuardData) Flush(context.Context, string) error { return nil }

type componentGuardSource struct {
	data  *componentGuardData
	opens int
}

func (s *componentGuardSource) Open(context.Context) (xhandler.Data, error) {
	s.opens++
	return s.data, nil
}
func (s *componentGuardSource) InvocationKey() any { return s }

type componentGuardReader struct {
	calls int
	run   func(context.Context, xhandler.Binder) error
}

func (r *componentGuardReader) Read(ctx context.Context, _ any, binder xhandler.Binder, _ sqlx.ParameterResolver) (any, error) {
	r.calls++
	if r.run != nil {
		if err := r.run(ctx, binder); err != nil {
			return nil, err
		}
	}
	return &componentGuardOutput{}, nil
}

func TestRuntimeWriteEligibilityComponentAdmissionRetainsAuthority(t *testing.T) {
	for _, mode := range []string{"custom GET", "first lookup during guard", "dependency replaced ctx", "reader dependency", "reader imperative", "reader allowed"} {
		t.Run(mode, func(t *testing.T) {
			parent := componentArtifact(t, componentSpec("GuardParent", "POST", "/guard-parent", nil), reflect.TypeFor[componentGuardInput](), reflect.TypeFor[componentGuardOutput]())
			child := componentArtifact(t, componentSpec("GuardCustom", "GET", "/guard-custom", []*spec.Parameter{{Name: "Value", Source: spec.BindSource{Kind: "guardBinding", Name: "value"}, TypeExpr: "string"}}), reflect.TypeFor[componentGuardBindingInput](), reflect.TypeFor[componentGuardOutput]())
			readerSpec := componentSpec("GuardReader", "GET", "/guard-reader", nil)
			readerType := reflect.TypeFor[componentGuardInput]()
			if mode == "reader dependency" {
				readerType = reflect.TypeFor[componentGuardDependencyInput]()
				readerSpec.Parameters = []*spec.Parameter{{Name: "Child", Source: spec.BindSource{Kind: "component", Name: "GET:/guard-custom"}, TypeExpr: "*componentGuardOutput"}}
			}
			reader := componentArtifact(t, readerSpec, readerType, reflect.TypeFor[componentGuardOutput]())
			customTarget := dexec.ComponentTarget{Component: child.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/guard-custom"}}
			readerTarget := dexec.ComponentTarget{Component: reader.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/guard-reader"}}
			parentTarget := dexec.ComponentTarget{Component: parent.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/guard-parent"}}
			parentSource := &componentGuardSource{data: &componentGuardData{}}
			childSource := &componentGuardSource{data: &componentGuardData{}}
			bindings, customCalls := 0, 0
			nativeReader := &componentGuardReader{}
			if mode == "reader imperative" {
				nativeReader.run = func(ctx context.Context, b xhandler.Binder) error {
					value, found, err := b.Lookup(ctx, dexec.ComponentInvokerKey)
					if err != nil || !found {
						t.Fatalf("reader invoker %v %v", found, err)
					}
					_, err = value.(dexec.ComponentInvoker).InvokeComponent(context.Background(), dexec.ComponentRequest{Target: customTarget})
					return err
				}
			}
			parentHandler := rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {

				var cached any
				var found bool
				var err error
				if mode != "first lookup during guard" {
					cached, found, err = inv.Binder.Lookup(context.Background(), dexec.ComponentInvokerKey)
					if err != nil || !found {
						t.Fatalf("parent invoker %v %v", found, err)
					}
				}
				finish, err := handlerengine.BeginWriteEligibility(ctx)
				if err != nil {
					return nil, err
				}

				defer finish()
				if mode == "first lookup during guard" {
					cached, found, err = inv.Binder.Lookup(context.Background(), dexec.ComponentInvokerKey)
					if err != nil || !found {
						t.Fatalf("first guarded invoker lookup %v %v", found, err)
					}
				}
				target := readerTarget
				if mode == "custom GET" || mode == "first lookup during guard" {
					target = customTarget
				}
				if mode == "dependency replaced ctx" {
					var dependency struct {
						Child *componentGuardOutput `bind:"kind=component,in=GET:/guard-custom,required"`
					}
					err = inv.Binder.Bind(context.Background(), &dependency)
				} else {
					_, err = cached.(dexec.ComponentInvoker).InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target})
				}
				if mode == "reader allowed" {
					if err != nil {
						return nil, err
					}
					if nativeReader.calls != 1 {
						t.Fatal("native reader not called")
					}
					if err := finish(); err != nil {
						return nil, err
					}
					return &componentGuardOutput{}, nil
				}
				if !errors.Is(err, handlerengine.ErrWriteEligibilityMutation) {
					t.Fatalf("child admission error %v", err)
				}
				if bindings != 0 || customCalls != 0 || childSource.opens != 0 || childSource.data.writes != 0 {
					t.Fatalf("child delegated: bindings=%d calls=%d opens=%d writes=%d", bindings, customCalls, childSource.opens, childSource.data.writes)
				}
				return nil, finish()
			})
			rt, err := NewRuntime([]*registry.RegisteredComponent{
				{Component: parent.Component, Input: parent.Input, OutputType: reflect.TypeFor[componentGuardOutput](), Handler: parentHandler, DataSource: parentSource},
				{Component: child.Component, Input: child.Input, OutputType: reflect.TypeFor[componentGuardOutput](), Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
					customCalls++
					return &componentGuardOutput{}, nil
				}), DataSource: childSource, Providers: []locator.Provider{handlerprovider.Named("guardBinding", func(context.Context, reflect.Type, string) (any, bool, error) { bindings++; return "unused", true, nil })}},
				{Component: reader.Component, Input: reader.Input, OutputType: reflect.TypeFor[componentGuardOutput](), Reader: nativeReader},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(context.Background())
			bindings = 0
			_, err = rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: parentTarget, Input: &componentGuardInput{}})
			if mode == "reader allowed" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, handlerengine.ErrWriteEligibilityMutation) {
				t.Fatal(err)
			}
		})
	}
}
