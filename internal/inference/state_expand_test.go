package inference

import (
	"os"
	"strings"
	"testing"
)

func TestStateExpandPreserveBuiltins(t *testing.T) {
	state := State{}
	state.Append(NewConstParameter("dataset", "ci_ads"))

	input := `SELECT * FROM ${dataset}.CI_AD_ORDER ao ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`

	got := state.ExpandPreserveBuiltins(input)
	if !strings.Contains(got, "ci_ads.CI_AD_ORDER") {
		t.Fatalf("expected const expansion, got: %s", got)
	}
	if !strings.Contains(got, `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`) {
		t.Fatalf("expected predicate builder to be preserved, got: %s", got)
	}
}

func TestStateExpandStripsBuiltins(t *testing.T) {
	state := State{}
	state.Append(NewConstParameter("dataset", "ci_ads"))

	input := `SELECT * FROM ${dataset}.CI_AD_ORDER ao ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`

	got := state.Expand(input)
	if !strings.Contains(got, "ci_ads.CI_AD_ORDER") {
		t.Fatalf("expected const expansion, got: %s", got)
	}
	if strings.Contains(got, `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`) {
		t.Fatalf("expected predicate builder to be stripped, got: %s", got)
	}
}

func TestStateExpand_StripsSupplyPerformancePredicateBuilders(t *testing.T) {
	data, err := os.ReadFile("/Users/awitas/go/src/github.vianttech.com/viant/steward/dql/inventory/sql/supply_performance.sql")
	if err != nil {
		t.Fatalf("read sql: %v", err)
	}
	state := State{}
	got := state.Expand(string(data))
	if strings.Contains(got, `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`) {
		t.Fatalf("expected WHERE predicate builder to be stripped, got: %s", got)
	}
	if strings.Contains(got, `${predicate.Builder().CombineOr($predicate.FilterGroup(1, "HAVING")).Build("HAVING")}`) {
		t.Fatalf("expected HAVING predicate builder to be stripped, got: %s", got)
	}
}

func TestStateExpand_StripsPredicateBuildersInWrappedSupplyPerformanceDQL(t *testing.T) {
	sqlData, err := os.ReadFile("/Users/awitas/go/src/github.vianttech.com/viant/steward/dql/inventory/sql/supply_performance.sql")
	if err != nil {
		t.Fatalf("read sql: %v", err)
	}
	dqlData, err := os.ReadFile("/Users/awitas/go/src/github.vianttech.com/viant/steward/dql/inventory/supply_performance.dql")
	if err != nil {
		t.Fatalf("read dql: %v", err)
	}
	combined := strings.Replace(string(dqlData), "${embed:sql/supply_performance.sql}", string(sqlData), 1)
	state := State{}
	got := state.Expand(combined)
	if strings.Contains(got, `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}`) {
		t.Fatalf("expected wrapped WHERE predicate builder to be stripped, got: %s", got)
	}
	if strings.Contains(got, `${predicate.Builder().CombineOr($predicate.FilterGroup(1, "HAVING")).Build("HAVING")}`) {
		t.Fatalf("expected wrapped HAVING predicate builder to be stripped, got: %s", got)
	}
}
