package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestWriterActionPolicyDirective(t *testing.T) {
	for _, tc := range []struct {
		expr  string
		valid bool
	}{
		{"r.*,writer_action_policy(r,'insert-delete')", true},
		{"r.*,writer_action_policy(r,'unknown')", false},
		{"r.*,writer_action_policy(other,'insert-delete')", false},
		{"r.*,writer_action_policy(r,'insert-delete') AS value", false},
		{"r.*,writer_action_policy(r,'insert-delete'),writer_action_policy(r,'insert-delete')", false},
	} {
		sql := "SELECT " + tc.expr + " FROM (SELECT id FROM records) r"
		v, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
		if (err == nil) != tc.valid {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		if err == nil && (v.WriterActionPolicy != "insert-delete" || v.Clone().WriterActionPolicy != v.WriterActionPolicy || strings.Contains(v.Source.SQL, "writer_action_policy")) {
			t.Fatal("authoring policy not cloned/removed from physical SQL")
		}
	}
}
