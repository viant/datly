package sql

import (
	"reflect"
	"testing"

	"github.com/viant/sqlparser"
)

func TestHLLMergePartialProjectionRoles(t *testing.T) {
	for _, tc := range []struct {
		expression string
		aggregate  bool
	}{
		{"HLL_COUNT.MERGE_PARTIAL(sketch)", true},
		{"hll_count.merge_partial(sketch)", true},
		{"COALESCE(HLL_COUNT.MERGE_PARTIAL(sketch),0)", true},
		{"HLL_COUNT.EXTRACT(sketch)", false},
		{"HLL_COUNT.EXTRACT(HLL_COUNT.MERGE_PARTIAL(sketch))", true},
		{"other.HLL_COUNT.MERGE_PARTIAL(sketch)", false},
		{"other.MERGE_PARTIAL(sketch)", false},
		{"MERGE_PARTIAL(sketch)", false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			text := "SELECT region, " + tc.expression + " AS total FROM sales GROUP BY region"
			parsed, err := sqlparser.ParseQuery(text)
			if err != nil {
				t.Fatal(err)
			}
			if actual := ContainsAggregate(parsed.List[1].Expr); actual != tc.aggregate {
				t.Fatalf("aggregate=%v want %v AST=%#v", actual, tc.aggregate, parsed.List[1].Expr)
			}
			fields, err := (CacheProjection{SQL: text}).Fields()
			if err != nil {
				t.Fatal(err)
			}
			if len(fields) != 2 || fields[0].Name != "region" || fields[1].Name != "total" || fields[0].DimensionKey == "" || (fields[1].MeasureKey != "") != tc.aggregate {
				t.Fatalf("fields=%+v", fields)
			}
			wrapped, err := (CacheProjection{SQL: "SELECT c.* FROM (" + text + ") c"}).Fields()
			if err != nil || !reflect.DeepEqual(fields, wrapped) {
				t.Fatalf("wrapper fields=%+v err=%v", wrapped, err)
			}
		})
	}
}
