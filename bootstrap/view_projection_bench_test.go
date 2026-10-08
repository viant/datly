package bootstrap

import (
	"github.com/viant/datly/spec"
	"testing"
)

func projectionBenchView() *spec.View {
	return &spec.View{Name: "Metrics", Source: &spec.ViewSource{SQL: "SELECT account_id,SUM(amount) AS total_spend FROM spend GROUP BY account_id"}, Columns: []*spec.Column{{Name: "AccountID", Source: "account_id", NameInferred: true}, {Name: "TotalSpend", Source: "total_spend", NameInferred: true}}}
}
func TestLinkedProjectionMetadataOnly(t *testing.T) {
	view := projectionBenchView()
	view.Source.SQL = "SELECT FROM vendor_specific_source"
	columns, err := (ViewProjection{View: view}).Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 2 || columns[0].Name != "account_id" || columns[1].Selector != "total_spend" {
		t.Fatalf("columns=%+v", columns)
	}
}
func BenchmarkLinkedViewProjection(b *testing.B) {
	view := projectionBenchView()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := (ViewProjection{View: view}).Columns(); err != nil {
			b.Fatal(err)
		}
	}
}
