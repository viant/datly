package reader

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

type selectorBinder struct{ selectors xstate.Selectors }

func (b selectorBinder) Bind(context.Context, any) error { return nil }
func (b selectorBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key != xhandler.SelectorsKey || b.selectors == nil {
		return nil, false, nil
	}
	return b.selectors, true, nil
}

func TestResolveInvocationSelectors_BindsByViewAndAppliesOverrideLast(t *testing.T) {
	type input struct {
		Fields []string
		Limit  int
	}
	root := &data.View{Spec: spec.View{Name: "users"}}
	child := &data.View{Spec: spec.View{Name: "accounts"}}
	root.Relations = []*data.Relation{{Of: &data.RelationRef{View: child}}}
	component := &spec.Component{Name: "Users"}
	plan, err := NewPlan(PlanConfig{
		RootView: root, ViewIndex: NewViewIndex(component, root),
		SelectorBindings: []SelectorBindingPlan{
			{View: root, Property: spec.SelectorPropertyFields, FieldIndex: []int{0}},
			{View: child, Property: spec.SelectorPropertyLimit, FieldIndex: []int{1}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{
		Component: component,
		Artifact:  plan,
	}
	provided := xstate.Selectors{&xstate.NamedSelector{Name: "accounts", Selector: xstate.Selector{Limit: 1, OrderBy: "id DESC"}}}
	bound := &input{Fields: []string{"ID", "Accounts"}, Limit: 9}

	selectors, err := resolveInvocationSelectors(context.Background(), session, reflect.ValueOf(bound), selectorBinder{selectors: provided})
	if err != nil {
		t.Fatalf("unexpected resolve error: %v", err)
	}
	if !reflect.DeepEqual(selectors.forView(root).Fields, bound.Fields) {
		t.Fatalf("unexpected root selector: %+v", selectors.forView(root))
	}
	if actual := selectors.forView(child); actual.Limit != 1 || actual.OrderBy != "id DESC" {
		t.Fatalf("named override did not replace derived child selector: %+v", actual)
	}
	bound.Fields[0] = "changed"
	if selectors.forView(root).Fields[0] != "ID" {
		t.Fatalf("selector fields must not alias input storage")
	}
	if provided[0].Limit != 1 {
		t.Fatalf("provided override was mutated")
	}
}

func TestResolveInvocationSelectors_RejectsUnknownOverride(t *testing.T) {
	root := &data.View{Spec: spec.View{Name: "users"}}
	component := &spec.Component{Name: "Users"}
	plan, err := NewPlan(PlanConfig{RootView: root, ViewIndex: NewViewIndex(component, root)})
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{
		Component: component,
		Artifact:  plan,
	}

	provided := xstate.Selectors{&xstate.NamedSelector{Name: "missing"}}
	_, err = resolveInvocationSelectors(context.Background(), session, reflect.ValueOf(&struct{}{}), selectorBinder{selectors: provided})
	if err == nil || !strings.Contains(err.Error(), "unknown view") {
		t.Fatalf("expected unknown view error, got %v", err)
	}
}
