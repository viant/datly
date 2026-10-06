package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestAuxiliaryNullDirectiveScopeAndClone(t *testing.T) {
	for _, tc := range []struct {
		name, sql   string
		root, valid bool
	}{
		{"root", "SELECT r.*,root_null_policy(r,'skip-auxiliary') FROM (SELECT id FROM (records)) r", true, true},
		{"writable root", "SELECT r.*,root_null_policy(r,'skip-auxiliary') FROM (SELECT id FROM records) r", true, false},
		{"nested", "SELECT r.*,c.*,nested_null_policy(c,'skip-auxiliary') FROM (SELECT id FROM parents) r JOIN (SELECT id,parent_id FROM (children)) c ON c.parent_id=r.id", false, true},
		{"writable nested", "SELECT r.*,c.*,nested_null_policy(c,'skip-auxiliary') FROM (SELECT id FROM parents) r JOIN (SELECT id,parent_id FROM children) c ON c.parent_id=r.id", false, false},
		{"wrong root target", "SELECT r.*,c.*,root_null_policy(c,'skip-auxiliary') FROM (SELECT id FROM parents) r JOIN (SELECT id,parent_id FROM (children)) c ON c.parent_id=r.id", false, false},
		{"duplicate", "SELECT r.*,root_null_policy(r,'skip-auxiliary'),root_null_policy(r,'skip-auxiliary') FROM (SELECT id FROM (records)) r", true, false},
		{"wrong enum", "SELECT r.*,root_null_policy(r,'skip') FROM (SELECT id FROM (records)) r", true, false},
		{"initial validation auxiliary", "SELECT r.*,root_null_policy(r,'initial-validation') FROM (SELECT id FROM (records)) r", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: tc.sql}}
			got, err := NewReader().Compile(ReadInput{View: input, SQL: tc.sql})
			if (err == nil) != tc.valid {
				t.Fatalf("err=%v", err)
			}
			if err != nil {
				return
			}
			clone := got.Clone()
			target := clone
			if !tc.root {
				target = clone.Relations[0].View
				if clone.NestedNullPolicy != "" {
					t.Fatal("policy leaked to parent")
				}
			}
			if !target.Auxiliary || (tc.root && target.RootNullPolicy != "skip-auxiliary") || (!tc.root && target.NestedNullPolicy != "skip-auxiliary") {
				t.Fatalf("lost exact policy: %+v", target)
			}
			if input.Source.SQL != tc.sql || input.RootNullPolicy != "" || input.NestedNullPolicy != "" {
				t.Fatal("authored contract mutated")
			}
			if strings.Contains(got.Source.SQL, "null_policy") {
				t.Fatal("directive retained as SQL")
			}
		})
	}
}
