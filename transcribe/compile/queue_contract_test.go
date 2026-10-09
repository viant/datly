package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestQueueContractDirective(t *testing.T) {
	for _, tc := range []struct {
		expr  string
		valid bool
	}{{"r.*,queue_contract(r,'source-row')", true}, {"r.*,queue_contract(r,'source-slice')", true}, {"r.*,queue_contract(r,'unknown')", false}, {"r.*,queue_contract(other,'source-row')", false}, {"r.*,queue_contract(r,'source-row') AS value", false}, {"r.*,queue_contract(r,'source-row'),queue_contract(r,'source-row')", false}, {"r.*,queue_contract(r,1)", false}} {
		sql := "SELECT " + tc.expr + " FROM (SELECT id FROM records) r"
		v, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
		if (err == nil) != tc.valid {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		if err == nil && ((v.QueueContract != "source-row" && v.QueueContract != "source-slice") || v.Clone().QueueContract != v.QueueContract || strings.Contains(v.Source.SQL, "queue_contract")) {
			t.Fatal("contract not cloned or stripped from SQL")
		}
	}
}
