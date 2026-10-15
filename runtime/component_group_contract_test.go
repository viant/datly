package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

// Exercise the registered immutable contract boundary. The unchanged Plan is
// deliberately never executed for rejected descriptors.
func groupContractWith(t *testing.T, c *registry.InputContract, route spec.RouteRef, change func([]bindly.BindingSpec)) *registry.InputContract {
	t.Helper()
	input, ok := c.ForRoute(route)
	if !ok {
		t.Fatal("missing route")
	}
	var bindings []bindly.BindingSpec
	for _, f := range input.Fields() {
		bindings = append(bindings, f.Binding())
	}
	change(bindings)
	projection, e := input.Plan().Projection()
	if e != nil {
		t.Fatal(e)
	}
	modified, e := registry.NewInputContract(c.Type(), projection, registry.RouteInput{Route: route, Plan: input.Plan(), Bindings: bindings})
	if e != nil {
		t.Fatal(e)
	}
	return modified
}
func TestScopedGroupCompiledClosureRejectsUnsupportedAlternatives(t *testing.T) {
	for _, mode := range []string{"when", "with", "async", "group", "default child", "multiple children", "adapted child", "custom leaf", "nested dependency", "cycle", "child mutation", "child provider classification"} {
		t.Run(mode, func(t *testing.T) {
			p := componentArtifact(t, componentSpec("ScopedClosure", "GET", "/scope-parent", nil), reflect.TypeFor[scopedNativeInput](), reflect.TypeFor[scopedNativeOutput]())
			p.Handler = custom.New[scopedNativeInput, scopedNativeOutput](scopedNativeContract{})
			pr, e := p.Registration(registry.RegisteredComponent{})
			if e != nil {
				t.Fatal(e)
			}
			c := componentArtifact(t, componentSpec("LeafClosure", "GET", "/scope-child", nil), reflect.TypeFor[scopedNativeChildInput](), reflect.TypeFor[scopedNativeChildOutput]())
			calls := 0
			cr := &registry.RegisteredComponent{Component: c.Component, Input: c.Input, Output: c.Output, OutputType: reflect.TypeFor[scopedNativeChildOutput](), Reader: groupedSeedReader{&calls}}
			route := spec.RouteRef{Method: "GET", Path: "/scope-parent"}
			switch mode {
			case "when", "with", "async", "group", "default child", "multiple children", "adapted child":
				pr.Input = groupContractWith(t, pr.Input, route, func(b []bindly.BindingSpec) {
					for i := range b {
						if b[i].Path == "Child" {
							switch mode {
							case "when":
								b[i].When = "true"
							case "with":
								b[i].With = "Credential"
							case "async":
								b[i].Async = true
							case "group":
								b[i].ResolutionGroup = &bindly.ResolutionGroupSpec{Name: "nested"}
							case "default child":
								b[i].Scope = ""
							case "adapted child":
								b[i].SourceType = reflect.TypeFor[string]()
							}
						}
					}
					if mode == "multiple children" {
						b[0].Location.Kind = "component"
						b[0].Location.In = "GET:/scope-child"
						b[0].Scope = "second"
					}
				})
			case "custom leaf":
				cr.Reader = nil
				cr.Handler = custom.NewFunc(func(context.Context, *scopedNativeChildInput) (*scopedNativeChildOutput, error) {
					t.Fatal("custom child executed")
					return nil, nil
				})
			case "nested dependency", "cycle":
				cr.Input = groupContractWith(t, pr.Input, route, func(b []bindly.BindingSpec) {
					if mode == "cycle" {
						b[1].Location.In = "GET:/scope-parent"
					}
				})
				// Give the child its exact route while retaining the component dependency.
				from, _ := cr.Input.ForRoute(route)
				projection, _ := from.Plan().Projection()
				var bindings []bindly.BindingSpec
				for _, f := range from.Fields() {
					bindings = append(bindings, f.Binding())
				}
				cr.Input, e = registry.NewInputContract(cr.Input.Type(), projection, registry.RouteInput{Route: spec.RouteRef{Method: "GET", Path: "/scope-child"}, Plan: from.Plan(), Bindings: bindings})
				if e != nil {
					t.Fatal(e)
				}
			case "child mutation":
				cr.Component.Settings = &spec.Settings{Mutation: "auto"}
			case "child provider classification":
				cr.Handler = rh.HandlerFunc(func(context.Context, rh.Invocation) (any, error) { t.Fatal("raw child executed"); return nil, nil })
			}
			rt, e := NewRuntime([]*registry.RegisteredComponent{pr, cr})
			if e != nil {
				if mode == "group" {
					t.Log("grouped metadata rejected by existing runtime registration before descriptor")
					return
				}
				t.Fatal(e)
			}
			defer rt.Shutdown(context.Background())
			if _, e = rt.scopedGroupDescriptor(t.Context(), pr, route, dexec.ComponentRequest{}, true); !errors.Is(e, drainowner.ErrBindingGroup) {
				t.Fatalf("closure admitted %s: %v", mode, e)
			}
			if calls != 0 {
				t.Fatal("rejected child executed")
			}
		})
	}
}

type scopedGroupLoader map[string]*registry.RegisteredComponent

func (l scopedGroupLoader) LoadComponent(_ context.Context, key spec.Key) (*registry.RegisteredComponent, error) {
	return l[key.String()], nil
}
func TestScopedGroupIndexedWrapperPreservesCompiledAuthority(t *testing.T) {
	parent := componentArtifact(t, componentSpec("IndexedScoped", "GET", "/scope-parent", nil), reflect.TypeFor[scopedNativeInput](), reflect.TypeFor[scopedNativeOutput]())
	parent.Handler = custom.New[scopedNativeInput, scopedNativeOutput](scopedNativeContract{})
	pr, e := parent.Registration(registry.RegisteredComponent{})
	if e != nil {
		t.Fatal(e)
	}
	leaf := componentArtifact(t, componentSpec("IndexedLeaf", "GET", "/scope-child", nil), reflect.TypeFor[scopedNativeChildInput](), reflect.TypeFor[scopedNativeChildOutput]())
	cr := &registry.RegisteredComponent{Component: leaf.Component, Input: leaf.Input, Output: leaf.Output, OutputType: reflect.TypeFor[scopedNativeChildOutput](), Reader: &recorderBoundReader{}}
	loader := scopedGroupLoader{pr.Component.Key.String(): pr, cr.Component.Key.String(): cr}
	rt, e := NewIndexedRuntime([]*spec.Component{pr.Component, cr.Component}, nil, loader)
	if e != nil {
		t.Fatal(e)
	}
	defer rt.Shutdown(context.Background())
	first, e := rt.LoadComponent(t.Context(), pr.Component.Key)
	if e != nil {
		t.Fatal(e)
	}
	second, e := rt.LoadComponent(t.Context(), pr.Component.Key)
	if e != nil {
		t.Fatal(e)
	}
	if first == second || first == pr || groupRegistration(first) != groupRegistration(second) {
		t.Fatal("registration wrapper used as compiled identity")
	}
	c1, e := rt.LoadComponent(t.Context(), cr.Component.Key)
	if e != nil {
		t.Fatal(e)
	}
	c2, e := rt.LoadComponent(t.Context(), cr.Component.Key)
	if e != nil {
		t.Fatal(e)
	}
	if c1 == c2 || c1.Reader == c2.Reader || groupRegistration(c1) != groupRegistration(c2) {
		t.Fatal("recorder-wrapped reader changed compiled identity")
	}
	route := spec.RouteRef{Method: "GET", Path: "/scope-parent"}
	for _, p := range []*registry.RegisteredComponent{first, second} {
		if _, e = rt.scopedGroupDescriptor(t.Context(), p, route, dexec.ComponentRequest{}, true); e != nil {
			t.Fatal(e)
		}
	}
	original := groupRegistration(first)
	for _, which := range []string{"component", "input", "output", "output type", "handler"} {
		t.Run(which, func(t *testing.T) {
			copy := *first
			switch which {
			case "component":
				v := *copy.Component
				copy.Component = &v
			case "input":
				v := *copy.Input
				copy.Input = &v
			case "output":
				v := *copy.Output
				copy.Output = &v
			case "output type":
				copy.OutputType = reflect.TypeFor[int]()
			case "handler":
				copy.Handler = custom.New[scopedNativeInput, scopedNativeOutput](scopedNativeContract{})
			}
			if groupRegistration(&copy) == original {
				t.Fatal("mismatched compiled field retained identity")
			}
		})
	}
}
