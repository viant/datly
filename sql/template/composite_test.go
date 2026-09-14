package template

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/sqlx/metadata/info"
)

func TestCompositeCriteriaUsesInvocationDialect(t *testing.T) {
	type key struct {
		Tenant int `sqlx:"tenant_id"`
		ID     int `sqlx:"id"`
	}
	type input struct{ Keys []key }
	program, err := (Compiler{Source: `SELECT id FROM records r WHERE $criteria.CompositeIn("r", $Keys)`, InputType: reflect.TypeFor[input](), Variables: []Variable{{Name: "Keys", FieldIndex: []int{0}}}}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	dialect := &info.Dialect{CompositeInRenderer: func(columns []string, rows int) string {
		if !reflect.DeepEqual(columns, []string{"r.tenant_id", "r.id"}) || rows != 2 {
			t.Errorf("dialect columns=%v rows=%d", columns, rows)
		}
		return "native_composite(?, ?, ?, ?)"
	}}
	actual, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(input{Keys: []key{{0, 0}, {2, 7}}}), View: ViewInput{Dialect: dialect}})
	if err != nil {
		t.Fatal(err)
	}
	if actual.SQL != "SELECT id FROM records r WHERE native_composite(?, ?, ?, ?)" || !reflect.DeepEqual(actual.Args, []any{int64(0), int64(0), int64(2), int64(7)}) {
		t.Fatalf("SQL=%q args=%v", actual.SQL, actual.Args)
	}
}
