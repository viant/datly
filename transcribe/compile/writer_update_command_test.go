package compile

import (
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

func TestWriterUpdateActionPolicyDirective(t *testing.T) {
	sql := "SELECT r.*,writer_action_policy(r,'update') FROM (SELECT id FROM records) r"
	v, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql})
	if err != nil {
		t.Fatal(err)
	}
	if v.WriterActionPolicy != "update" || strings.Contains(v.Source.SQL, "writer_action_policy") {
		t.Fatalf("policy: %+v", v)
	}
}
