package reader

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	xhandler "github.com/viant/xdatly/handler"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

type selectorOwnerInput struct {
	Chosen []string
	Other  []string
}
type selectorOwnerOutput struct {
	legacy.Status
	Rows []int
}
type selectorOwnerPointerOutput struct {
	*legacy.Status
	Rows []int
}
type selectorOwnerModernOutput struct {
	modern.Status
	Rows []int
}

func ownerSession(t *testing.T) *Session {
	t.Helper()
	component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}, Parameters: []*spec.Parameter{
		{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}},
		{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		{Name: "Chosen", Source: spec.BindSource{Kind: "query", Name: "pick"}, QuerySelector: &spec.QuerySelectorBinding{View: "records", Property: spec.SelectorPropertyFields}},
	}}
	root := &data.View{Spec: *component.RootView}
	plan, e := NewPlan(PlanConfig{RootView: root, ViewIndex: NewViewIndex(component, root), OutputViewField: "Rows", OutputStatusField: "Status", SelectorBindings: []SelectorBindingPlan{{View: root, Property: spec.SelectorPropertyFields, FieldIndex: []int{0}}}})
	if e != nil {
		t.Fatal(e)
	}
	s := &Session{Component: component, Artifact: plan, InputType: reflect.TypeFor[selectorOwnerInput](), OutputType: reflect.TypeFor[selectorOwnerOutput](), outputAccessors: &outputAccessors{}}
	s.outputAccessors.selectorOwner = s.compileSelectorOwner()
	return s
}
func TestSelectorDeclaredOwnerEligibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*Session)
		eligible bool
	}{
		{"renamed-query", func(*Session) {}, true},
		{"pointer-status", func(s *Session) { s.OutputType = reflect.TypeFor[selectorOwnerPointerOutput]() }, true},
		{"modern", func(s *Session) { s.OutputType = reflect.TypeFor[selectorOwnerModernOutput]() }, false},
		{"undeclared-status", func(s *Session) { s.Component.Parameters = s.Component.Parameters[1:] }, false},
		{"undeclared-view", func(s *Session) {
			s.Component.Parameters = append(s.Component.Parameters[:1], s.Component.Parameters[2:]...)
		}, false},
		{"ambiguous-status", func(s *Session) { s.Component.Parameters = append(s.Component.Parameters, s.Component.Parameters[0]) }, false},
		{"ambiguous-view", func(s *Session) { s.Component.Parameters = append(s.Component.Parameters, s.Component.Parameters[1]) }, false},
		{"missing-query-key", func(s *Session) { s.Component.Parameters[2].Source.Name = "" }, false},
		{"whitespace-query-key", func(s *Session) { s.Component.Parameters[2].Source.Name = " " }, false},
		{"trimmed-query-key-mismatch", func(s *Session) { s.Component.Parameters[2].Source.Name = " pick " }, false},
		{"path", func(s *Session) { s.Component.Parameters[2].Source.Kind = "path" }, false},
		{"header", func(s *Session) { s.Component.Parameters[2].Source.Kind = "header" }, false},
		{"in", func(s *Session) { s.Component.Parameters[2].Source.Kind = "in" }, false},
		{"wrong-property", func(s *Session) { s.Component.Parameters[2].QuerySelector.Property = spec.SelectorPropertyOrderBy }, false},
		{"wrong-index", func(s *Session) { s.Artifact.SelectorBindings[0].FieldIndex = []int{1} }, false},
		{"wrong-view", func(s *Session) { s.Artifact.SelectorBindings[0].View = &data.View{Spec: spec.View{Name: "records"}} }, false},
		{"multiple-fields", func(s *Session) {
			s.Artifact.SelectorBindings = append(s.Artifact.SelectorBindings, s.Artifact.SelectorBindings[0])
		}, false},
		{"multiple-declarations", func(s *Session) { s.Component.Parameters = append(s.Component.Parameters, s.Component.Parameters[2]) }, false},
		{"root-name-mismatch", func(s *Session) { s.Component.RootView.Name = "foreign" }, false},
		{"custom", func(s *Session) { s.Component.Routes = []*spec.Route{{Handler: "custom"}} }, false},
		{"mutation", func(s *Session) { s.Component.Settings = &spec.Settings{Mutation: "patch"} }, false},
		{"auxiliary", func(s *Session) { s.Artifact.Root.View.Spec.Auxiliary = true }, false},
		{"inmemory", func(s *Session) { s.Artifact.Root.View.Spec.InMemory = true }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := ownerSession(t)
			tc.change(s)
			owner := s.compileSelectorOwner()
			if (owner != nil) != tc.eligible {
				t.Fatalf("eligible=%t want %t", owner != nil, tc.eligible)
			}
			if owner != nil && (owner.name != "Chosen" || owner.path != "Chosen" || owner.kind != "query" || owner.in != "pick") {
				t.Fatal("canonical query owner changed")
			}
		})
	}
}

type countedSelectorBinder struct {
	calls  int
	values xstate.Selectors
}

func (*countedSelectorBinder) Bind(context.Context, any) error { return nil }
func (b *countedSelectorBinder) Lookup(context.Context, xhandler.ValueKey) (any, bool, error) {
	b.calls++
	return b.values, len(b.values) > 0, nil
}
func TestSelectorOriginAndPrelookupTiming(t *testing.T) {
	s := ownerSession(t)
	b := &countedSelectorBinder{values: xstate.Selectors{&xstate.NamedSelector{Name: "records", Selector: xstate.Selector{Fields: []string{"id"}}}}}
	_, err := resolveInvocationSelectors(t.Context(), s, reflect.ValueOf(&selectorOwnerInput{Chosen: []string{""}}), b)
	if err == nil || b.calls != 0 {
		t.Fatal("prelookup failure ran provider")
	}
	var explicit *modern.Error
	if !errors.As(err, &explicit) || explicit.Payload == nil {
		t.Fatal("prelookup presentation changed")
	}
	_, err = resolveInvocationSelectors(t.Context(), s, reflect.ValueOf(&selectorOwnerInput{Chosen: []string{"id"}}), b)
	if err != nil || b.calls != 1 || s.selectorOwned {
		t.Fatal("capability override acquired ownership")
	}
	s.selectorOwned = false
	b = &countedSelectorBinder{}
	_, err = resolveInvocationSelectors(t.Context(), s, reflect.ValueOf(&selectorOwnerInput{Chosen: []string{"id"}}), b)
	if err != nil || b.calls != 1 || !s.selectorOwned {
		t.Fatal("declared origin missing")
	}
	native := &dsql.UnknownProjectionColumnError{Column: "${error}"}
	owned := s.selectorError(s.Artifact.Root.View, native, s.selectorOwned)
	var binding *bindly.BindingError
	if !errors.As(owned, &binding) || binding.Message != "" || binding.Error() != "Bad Request" || binding.Name != "Chosen" || !errors.Is(owned, native) {
		t.Fatal("unsafe owner/cause projection")
	}
	if s.selectorError(&data.View{}, native, true) != nil {
		t.Fatal("child failure acquired root identity")
	}
}
