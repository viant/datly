package builder

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func tableSourceFixture() *data.View {
	return &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "people"}, Namespace: "p"}, Columns: []*data.Column{{Name: "ID", Column: "p.id"}, {Name: "Name", Column: "p.name"}}}
}

func TestTableSourceIdentifierValidationAndComputedFallback(t *testing.T) {
	for _, table := range []string{"people", "schema.people", "`schema`.`people`"} {
		view := tableSourceFixture()
		view.Spec.Source.Table = table
		options := &builderOptions{source: view.Spec.Source, view: view}
		if err := NewBuilder().resolveSourceSQL(options); err != nil {
			t.Fatalf("table %q: %v", table, err)
		}
		if options.sqlText != "SELECT p.id AS id, p.name AS name FROM "+table+" p" {
			t.Fatalf("table or aliases lost: %s", options.sqlText)
		}
	}
	for _, table := range []string{"people; DELETE FROM people", "(SELECT", "people WHERE"} {
		view := tableSourceFixture()
		view.Spec.Source.Table = table
		if err := NewBuilder().resolveSourceSQL(&builderOptions{source: view.Spec.Source, view: view}); err == nil {
			t.Errorf("invalid table accepted: %q", table)
		}
	}
	view := tableSourceFixture()
	view.Columns[0].Expression = "COALESCE(p.id, 0)"
	if err := NewBuilder().resolveSourceSQL(&builderOptions{source: view.Spec.Source, view: view}); err != nil {
		t.Fatalf("computed projection rejected: %v", err)
	}
	view.Columns[0].Expression = "COALESCE("
	if err := NewBuilder().resolveSourceSQL(&builderOptions{source: view.Spec.Source, view: view}); err == nil {
		t.Fatal("malformed computed projection accepted")
	}
}

func BenchmarkTableSourcePreparation(b *testing.B) {
	view := tableSourceFixture()
	b.Run("source", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			options := &builderOptions{source: view.Spec.Source, view: view}
			if err := NewBuilder().resolveSourceSQL(options); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("build", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := NewBuilder().Build(context.Background(), WithBuilderView(view), WithBuilderInput(reflect.ValueOf(struct{}{}))); err != nil {
				b.Fatal(err)
			}
		}
	})
}
