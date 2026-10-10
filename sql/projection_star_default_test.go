package sql

import (
	"strings"
	"testing"

	"github.com/viant/datly/data"
)

func TestDefaultStarWithoutFallbackPreservesValidation(t *testing.T) {
	const source = "SELECT * FROM (SELECT id,amount FROM records) r"
	for _, column := range []*data.Column{
		{Name: "amount", Column: "amount"},
		{Name: "amount", Column: "amount", Nullable: true},
		{Name: "amount", Column: "amount", NullFallback: "0"},
		{Name: "amount", Column: "amount", Nullable: true, NullFallback: "  "},
	} {
		view := &data.View{Columns: []*data.Column{nil, {Name: "id", Column: "id"}, column}}
		got, err := (SelectorProjection{SQL: source, View: view}).Prepare(nil)
		if err != nil || got.Render(got.Source) != source {
			t.Fatalf("unchanged default lost: %+v %v", got, err)
		}
		_, err = (SelectorProjection{SQL: "SELECT * FROM (SELECT id AS duplicate,amount AS duplicate FROM records) r", View: view}).Prepare(nil)
		if err == nil {
			t.Fatal("duplicate derived output admitted")
		}
		if _, err = (SelectorProjection{SQL: source, View: view}).Prepare([]string{"missing"}); err == nil {
			t.Fatal("unknown selected output admitted")
		}
	}
	view := &data.View{Columns: []*data.Column{{Name: "id", Column: "id"}, {Name: "amount", Column: "amount", Nullable: true, NullFallback: "0"}}}
	got, err := (SelectorProjection{SQL: source, View: view}).Prepare(nil)
	if err != nil || !strings.Contains(got.Render(got.Source), "COALESCE(amount, 0)") {
		t.Fatalf("fallback lost: %+v %v", got, err)
	}
}
