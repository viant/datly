package parser

import (
	"os"
	"strings"
	"testing"

	"github.com/viant/datly/internal/inference"
)

func TestTemplate_Sanitize_SupplyPerformancePredicateBuilderPreserved(t *testing.T) {
	data, err := os.ReadFile("/Users/awitas/go/src/github.vianttech.com/viant/steward/dql/inventory/sql/supply_performance.sql")
	if err != nil {
		t.Fatalf("read sql: %v", err)
	}
	state := inference.State{}
	tmpl, err := NewTemplate(string(data), &state)
	if err != nil {
		t.Fatalf("new template: %v", err)
	}
	actual := tmpl.Sanitize()
	if !strings.Contains(actual, `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`) {
		t.Fatalf("expected WHERE predicate builder to survive sanitize, got: %s", actual)
	}
	if !strings.Contains(actual, `${predicate.Builder().CombineOr($predicate.FilterGroup(1, "HAVING")).Build("HAVING")}`) {
		t.Fatalf("expected HAVING predicate builder to survive sanitize, got: %s", actual)
	}
}
