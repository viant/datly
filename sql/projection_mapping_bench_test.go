package sql

import (
	"fmt"
	"github.com/viant/datly/data"
	"testing"
)

func BenchmarkWideProjectionMappings(b *testing.B) {
	columns := make([]ProjectionColumn, 120)
	view := &data.View{}
	for i := range columns {
		column := fmt.Sprintf("column_%d", i)
		columns[i] = ProjectionColumn{names: ProjectionNames{column, "source." + column}, output: column}
		view.SelectorFields = append(view.SelectorFields, data.SelectorField{GoName: fmt.Sprintf("Column%d", i), PublicName: fmt.Sprintf("field%d", i), Column: column})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		current := append([]ProjectionColumn(nil), columns...)
		(SelectorProjection{View: view}).applyMappings(current)
		if !current[119].Matches("field119") {
			b.Fatal("mapping lost")
		}
	}
}

func TestProjectionMappingsRetainUniqueSourceAuthority(t *testing.T) {
	columns := []ProjectionColumn{
		{names: ProjectionNames{"first", "shared", "shared", "source.one"}, output: "first"},
		{names: ProjectionNames{"second", "shared", "source.two"}, output: "second"},
	}
	view := &data.View{SelectorFields: []data.SelectorField{
		{GoName: "Ambiguous", Column: "shared"},
		{GoName: "Unique", Column: "SOURCE.ONE"},
		{GoName: "Transitive", Column: "Unique"},
		{GoName: "WrongNamespace", Column: "other.one"},
		{GoName: "Holder", Column: "source.one", Holder: true},
	}}
	(SelectorProjection{View: view}).applyMappings(columns)
	if !columns[0].Matches("Unique") {
		t.Fatal("unique qualified source mapping lost")
	}
	for _, name := range []string{"Ambiguous", "Transitive", "WrongNamespace", "Holder"} {
		for _, column := range columns {
			if column.Matches(name) {
				t.Fatalf("unauthorized mapping %s", name)
			}
		}
	}
}
