package reader

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	xstate "github.com/viant/xdatly/state"
)

func TestRootProjection(t *testing.T) {
	relation := &data.Relation{
		Holder: "Accounts",
		On: data.Links{
			nil,
			data.NewLink("", "user_id", "UserID"),
			data.NewLink("", "user_id", "UserID"),
		},
	}

	view := &data.View{Relations: []*data.Relation{nil, relation}}
	selector := &xstate.Selector{
		Columns: []string{"  ", "Name", "Accounts", "Name"},
		Fields:  []string{"Accounts"},
	}

	actual := viewProjection(view, selector)
	expected := []string{"Name", "user_id"}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("unexpected root projection: got %+v, want %+v", actual, expected)
	}
}

func TestSelectorStatelet_IgnoresBlankProjection(t *testing.T) {
	if actual := selectorStatelet(nil, &xstate.Selector{Fields: []string{"", "  "}}); actual != nil {
		t.Fatalf("blank projection must not prune relations: %+v", actual)
	}
}

func TestFieldSelectionModeIsScopedToViewAndTrustedColumns(t *testing.T) {
	root := &data.View{Spec: spec.View{Selector: &spec.Selector{FieldSelection: spec.FieldSelectionOutputOnly}}}
	child := &data.View{Spec: spec.View{Selector: &spec.Selector{}}}
	selector := &xstate.Selector{Fields: []string{"Name", "Children"}}
	if projection := viewProjection(root, selector); projection != nil {
		t.Fatalf("output-only SQL projection: %v", projection)
	}
	if state := selectorStatelet(root, selector); state != nil {
		t.Fatalf("output-only relation pruning: %v", state)
	}
	if projection := viewProjection(child, selector); !reflect.DeepEqual(projection, selector.Fields) {
		t.Fatalf("child projection: %v", projection)
	}
	if state := selectorStatelet(child, selector); state == nil || !state.Has("Children") {
		t.Fatal("child retains its selector")
	}
	selector.Columns = []string{"ID"}
	if projection := viewProjection(root, selector); !reflect.DeepEqual(projection, selector.Columns) {
		t.Fatalf("trusted columns: %v", projection)
	}
	if state := selectorStatelet(root, selector); state == nil || state.Has("Children") {
		t.Fatal("trusted columns retain relation pruning")
	}
	if !reflect.DeepEqual(selector.Fields, []string{"Name", "Children"}) {
		t.Fatal("request Fields mutated")
	}
	if child.Spec.Selector.FieldSelection != "" {
		t.Fatal("sibling policy mutated")
	}
}
