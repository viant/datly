package reader

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
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
	if actual := selectorStatelet(&xstate.Selector{Fields: []string{"", "  "}}); actual != nil {
		t.Fatalf("blank projection must not prune relations: %+v", actual)
	}
}
