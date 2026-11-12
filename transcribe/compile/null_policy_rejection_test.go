package compile

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestRemovedAuxiliaryNullPolicyDirectiveRejected(t *testing.T) {
	for _, sql := range []string{
		"SELECT r.*,root_null_policy(r,'skip-auxiliary') FROM (SELECT id FROM (records)) r",
		"SELECT r.*,c.*,nested_null_policy(c,'skip-auxiliary') FROM (SELECT id FROM parents) r JOIN (SELECT id,parent_id FROM (children)) c ON c.parent_id=r.id",
	} {
		if _, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql}}, SQL: sql}); err == nil {
			t.Fatal("removed policy admitted")
		}
	}
}
