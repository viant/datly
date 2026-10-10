package sql

import (
	"os"
	"strings"
	"testing"

	"github.com/viant/datly/data"
)

func BenchmarkDefaultNestedStarProjection(b *testing.B) {
	source := "SELECT * FROM (SELECT p.id,COALESCE(p.name,'') AS name,d.count FROM parents p LEFT JOIN (SELECT parent_id,COUNT(*) AS count FROM details GROUP BY parent_id) d ON d.parent_id=p.id) records"
	if path := os.Getenv("DATLY_PROJECTION_BENCH_SQL"); path != "" {
		body, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		source = strings.ReplaceAll(string(body), `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("AND")}`, " AND cr.ID = ?")
		// Fixed lookup inputs: Id is present, dayRange is 300, and the optional
		// region group has no predicate. Only the benchmark fixture is bound.
		source = strings.ReplaceAll(source, "#if($Has.Id) true #else false #end", "true")
		source = strings.ReplaceAll(source, "$DayRange", "300")
		source = strings.ReplaceAll(source, `${predicate.Builder().CombineOr($predicate.FilterGroup(2, "AND")).Build("AND")}`, "")
	}
	view := &data.View{Columns: []*data.Column{{Name: "Id", Column: "ID"}, {Name: "Name", Column: "name"}, {Name: "Count", Column: "count"}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := (SelectorProjection{SQL: source, View: view}).Prepare(nil)
		if err != nil {
			b.Fatal(err)
		}
		if result.Render(result.Source) != source {
			b.Fatal("default query changed")
		}
	}
}
