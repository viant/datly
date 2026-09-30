package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestMutationPredicateAnnotation(t *testing.T) {
	type useCase struct {
		desc, input string
		expect      bool
	}
	for _, tc := range []useCase{
		{"group zero", "r.*,mutation_predicate(r,0)", true},
		{"group seven", "r.*,mutation_predicate(r,7)", true},
		{"missing view", "r.*,mutation_predicate(other,7)", false},
		{"negative", "r.*,mutation_predicate(r,-1)", false},
		{"fraction", "r.*,mutation_predicate(r,1.5)", false},
		{"string", "r.*,mutation_predicate(r,'7')", false},
		{"alias", "r.*,mutation_predicate(r,7) AS alias", false},
		{"nested", "r.*,COALESCE(mutation_predicate(r,7),0)", false},
		{"duplicate", "r.*,mutation_predicate(r,7),mutation_predicate(r,8)", false},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			sql := "SELECT " + tc.input + " FROM (SELECT id FROM records) r"
			got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
			if (err == nil) != tc.expect {
				t.Fatalf("error=%v", err)
			}
			if err != nil {
				return
			}
			if got.MutationPredicateGroup == nil || strings.Contains(got.Source.SQL, "mutation_predicate(") {
				t.Fatalf("metadata or SQL=%+v", got)
			}
			clone := got.Clone()
			*clone.MutationPredicateGroup = 99
			if *got.MutationPredicateGroup == 99 {
				t.Fatal("clone aliases group metadata")
			}
		})
	}
}
