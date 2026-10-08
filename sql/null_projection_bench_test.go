package sql

import (
	"strings"
	"testing"

	"github.com/viant/datly/data"
)

func TestNullProjectionUsesColumnFallbackMetadata(t *testing.T) {
	const source = "SELECT id, amount FROM spend"
	for _, column := range []*data.Column{
		{Name: "amount", Column: "amount"},
		{Name: "amount", Column: "amount", Nullable: true},
		{Name: "amount", Column: "amount", NullFallback: "0"},
		{Name: "amount", Column: "amount", Nullable: true, NullFallback: "  "},
	} {
		actual, err := applyNullProjection(source, &data.View{Columns: []*data.Column{nil, column}})
		if err != nil || actual != source {
			t.Fatalf("no fallback rewrite expected for %+v: %q %v", column, actual, err)
		}
	}
	view := &data.View{Columns: []*data.Column{{Name: "amount", Column: "amount", Nullable: true, NullFallback: "0"}}}
	actual, err := applyNullProjection(source, view)
	if err != nil || !strings.Contains(actual, "COALESCE(amount, 0)") {
		t.Fatalf("nullable fallback lost: %q %v", actual, err)
	}
}

func BenchmarkNullProjection(b *testing.B) {
	const source = "SELECT id, amount FROM spend"
	view := &data.View{Columns: []*data.Column{{Name: "id", Column: "id"}, {Name: "amount", Column: "amount"}}}
	b.Run("rewrite", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := applyNullProjection(source, view); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("prepare", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := (SelectorProjection{SQL: source, View: view}).Prepare([]string{"id"}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
