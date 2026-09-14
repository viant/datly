package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestCompileSelectorBindings_ResolvesExactViewAndField(t *testing.T) {
	type input struct {
		Fields []string
		Limit  int
	}
	child := &data.View{Spec: spec.View{Name: "accounts", Key: spec.Key{Name: "Accounts"}}}
	root := &data.View{Spec: spec.View{Name: "users"}, Relations: []*data.Relation{{
		Of: &data.RelationRef{View: child},
	}},
	}
	component := &spec.Component{
		Name: "Users",
		Parameters: []*spec.Parameter{
			{Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "users", Property: spec.SelectorPropertyFields}},
			{Name: "Limit", Source: spec.BindSource{Kind: "query", Name: "limit"}, QuerySelector: &spec.QuerySelectorBinding{View: "accounts", Property: spec.SelectorPropertyLimit}},
		},
	}

	inputBindings, err := testBindings(component, reflect.TypeOf(input{}))
	if err != nil {
		t.Fatalf("unexpected binding error: %v", err)
	}
	bindings, err := CompileSelectorBindings(component, reflect.TypeOf(input{}), root, inputBindings)
	if err != nil {
		t.Fatalf("unexpected compile error: %v", err)
	}
	if len(bindings) != 2 {
		t.Fatalf("expected two bindings, got %d", len(bindings))
	}
	if bindings[0].View != root || bindings[0].Property != spec.SelectorPropertyFields {
		t.Fatalf("unexpected root binding: %+v", bindings[0])
	}
	if bindings[1].View != child || bindings[1].Property != spec.SelectorPropertyLimit {
		t.Fatalf("unexpected child binding: %+v", bindings[1])
	}
}

func TestCompileSelectorBindings_RejectsInvalidMetadata(t *testing.T) {
	type input struct {
		Fields string
		Limit  int
		Other  int
	}
	root := &data.View{Spec: spec.View{Name: "users"}}
	testCases := []struct {
		name      string
		params    []*spec.Parameter
		errorPart string
	}{
		{
			name: "unknown view",
			params: []*spec.Parameter{{Name: "Limit", Source: spec.BindSource{Kind: "query", Name: "limit"}, QuerySelector: &spec.QuerySelectorBinding{
				View: "missing", Property: spec.SelectorPropertyLimit,
			}}},
			errorPart: "unknown view",
		},
		{
			name: "incompatible field",
			params: []*spec.Parameter{{Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{
				View: "users", Property: spec.SelectorPropertyFields,
			}}},
			errorPart: "cannot use string",
		},
		{
			name: "duplicate property",
			params: []*spec.Parameter{
				{Name: "Limit", Source: spec.BindSource{Kind: "query", Name: "limit"}, QuerySelector: &spec.QuerySelectorBinding{View: "users", Property: spec.SelectorPropertyLimit}},
				{Name: "Other", Source: spec.BindSource{Kind: "query", Name: "other"}, QuerySelector: &spec.QuerySelectorBinding{View: "users", Property: spec.SelectorPropertyLimit}},
			},
			errorPart: "duplicate",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			component := &spec.Component{Name: "Users", Parameters: testCase.params}
			inputBindings, bindingErr := testBindings(component, reflect.TypeOf(input{}))
			if bindingErr != nil {
				t.Fatalf("unexpected binding error: %v", bindingErr)
			}
			_, err := CompileSelectorBindings(component, reflect.TypeOf(input{}), root, inputBindings)
			if err == nil || !strings.Contains(err.Error(), testCase.errorPart) {
				t.Fatalf("expected error containing %q, got %v", testCase.errorPart, err)
			}
		})
	}
}

func TestCompileSelectorBindings_AllowsUnreferencedDuplicateAliases(t *testing.T) {
	type input struct {
		Limit int
	}
	first := &data.View{Spec: spec.View{Name: "items"}}
	second := &data.View{Spec: spec.View{Name: "items"}}
	root := &data.View{Spec: spec.View{Name: "root"}, Relations: []*data.Relation{
		{Of: &data.RelationRef{View: first}},
		{Of: &data.RelationRef{View: second}},
	}}

	empty := &spec.Component{Name: "Root"}
	bindings, err := CompileSelectorBindings(empty, reflect.TypeOf(input{}), root, nil)
	if err != nil || len(bindings) != 0 {
		t.Fatalf("unreferenced duplicate aliases must remain valid, bindings=%+v err=%v", bindings, err)
	}
	component := &spec.Component{Name: "Root", Parameters: []*spec.Parameter{{
		Name: "Limit", Source: spec.BindSource{Kind: "query", Name: "limit"}, QuerySelector: &spec.QuerySelectorBinding{View: "items", Property: spec.SelectorPropertyLimit},
	}}}
	inputBindings, bindingErr := testBindings(component, reflect.TypeOf(input{}))
	if bindingErr != nil {
		t.Fatalf("unexpected binding error: %v", bindingErr)
	}
	_, err = CompileSelectorBindings(component, reflect.TypeOf(input{}), root, inputBindings)
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected referenced duplicate alias to fail, got %v", err)
	}
}
