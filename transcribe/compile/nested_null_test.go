package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestNestedNullPolicyDirective(t *testing.T) {
	for _, tc := range []struct {
		annotation string
		valid      bool
	}{
		{"nested_null_policy(c,'initial-validation')", true},
		{"nested_null_policy(r,'initial-validation')", false},
		{"nested_null_policy(c,'bad')", false},
		{"nested_null_policy(c,true)", false},
		{"nested_null_policy(c,'initial-validation') AS bad", false},
		{"nested_null_policy(c,'initial-validation'),nested_null_policy(c,'initial-validation')", false},
	} {
		sql := "SELECT r.*,c.*," + tc.annotation + " FROM (SELECT id FROM parents) r JOIN (SELECT id,parent_id FROM children) c ON c.parent_id=r.id"
		got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
		if (err == nil) != tc.valid {
			t.Fatalf("%s: %v", tc.annotation, err)
		}
		if err == nil && (got.NestedNullPolicy != "" || len(got.Relations) != 1 || got.Relations[0].View.NestedNullPolicy != "initial-validation" || strings.Contains(got.Source.SQL, "nested_null_policy")) {
			t.Fatal("policy not scoped to child")
		}
	}
}
