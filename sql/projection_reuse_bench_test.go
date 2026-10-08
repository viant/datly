package sql

import (
	"testing"

	"github.com/viant/datly/data"
)

func nullableProjectionView() *data.View {
	return &data.View{Columns: []*data.Column{{Name: "id", Column: "id"}, {Name: "amount", Column: "amount", Nullable: true, NullFallback: "0"}}}
}

func TestNullableProjectionReusesOnlyUnchangedStatements(t *testing.T) {
	for _, test := range []struct {
		selected []string
		want     string
	}{
		{[]string{"id", "amount"}, "SELECT id, COALESCE(amount, 0) AS amount FROM spend"},
		{[]string{"amount"}, "SELECT COALESCE(amount, 0) AS amount FROM spend"},
		{[]string{"id"}, "SELECT id FROM spend"},
	} {
		actual, err := (SelectorProjection{SQL: "SELECT id, amount FROM spend", View: nullableProjectionView()}).Prepare(test.selected)
		if err != nil || actual.Source != test.want {
			t.Fatalf("selection %v: %+v %v, want %q", test.selected, actual, err, test.want)
		}
	}
	// Projection-only fallback is not a validated statement and cannot supply
	// the nullable rewrite owner with an AST for this unresolved suffix.
	const unresolved = "SELECT id, amount FROM spend WHERE id=!!!"
	actual, err := (SelectorProjection{SQL: unresolved, View: nullableProjectionView()}).Prepare([]string{"id", "amount"})
	if err != nil || actual.Source != unresolved {
		t.Fatalf("projection-only AST was reused: %+v %v", actual, err)
	}
}

func BenchmarkNullableProjectionPreparation(b *testing.B) {
	view := nullableProjectionView()
	selected := []string{"id", "amount"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := (SelectorProjection{SQL: "SELECT id, amount FROM spend", View: view}).Prepare(selected); err != nil {
			b.Fatal(err)
		}
	}
}
