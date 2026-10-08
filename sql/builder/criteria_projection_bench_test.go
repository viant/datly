package builder

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/criteria"
	xstate "github.com/viant/xdatly/state"
)

func criteriaProjectionOptions(expression string) *builderOptions {
	return &builderOptions{sqlText: "SELECT p.id AS external_id, p.name FROM people p", selector: &xstate.Selector{Criteria: expression}, selectorPolicy: &spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"external_id"}}, criteriaCompiler: &criteria.Compiler{Columns: map[string]criteria.Column{"p.id": {Expression: "p.id", Type: reflect.TypeFor[int]()}}}}
}

func TestCriteriaProjectionPreservesAliasesTypesAndPolicy(t *testing.T) {
	options := criteriaProjectionOptions("external_id > 1")
	if err := NewBuilder().prepareCriteria(options); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(options.selector.Criteria, "p.id") || !reflect.DeepEqual(options.selector.Placeholders, []any{1}) {
		t.Fatalf("alias or typed binding lost: %+v", options.selector)
	}
	for _, expression := range []string{"external_id > 'wrong-type'", "p.name = 'hidden'", "external_id > 1; DELETE FROM people"} {
		if err := NewBuilder().prepareCriteria(criteriaProjectionOptions(expression)); err == nil {
			t.Errorf("invalid criterion accepted: %s", expression)
		}
	}
}

func BenchmarkCriteriaProjectionPreparation(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := NewBuilder().prepareCriteria(criteriaProjectionOptions("external_id > 1")); err != nil {
			b.Fatal(err)
		}
	}
}
