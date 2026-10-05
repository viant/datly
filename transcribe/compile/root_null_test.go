package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestRootNullPolicyDirective(t *testing.T) {
	for _, tc := range []struct {
		expr  string
		valid bool
	}{{"r.*,root_null_policy(r,'initial-validation')", true}, {"r.*,root_null_policy(r,'bad')", false}, {"r.*,root_null_policy(other,'initial-validation')", false}, {"r.*,root_null_policy(r,true)", false}, {"r.*,root_null_policy(r,'initial-validation') AS value", false}, {"r.*,root_null_policy(r,'initial-validation'),root_null_policy(r,'initial-validation')", false}} {
		sql := "SELECT " + tc.expr + " FROM (SELECT id FROM records) r"
		got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
		if (err == nil) != tc.valid {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		if err == nil && (got.RootNullPolicy != "initial-validation" || strings.Contains(got.Source.SQL, "root_null_policy")) {
			t.Fatal("policy lost or retained in SQL")
		}
	}
}
