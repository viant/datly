package compiler

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestCanonicalKeysRespectAuthoredSQLXIdentity(t *testing.T) {
	view := &spec.View{Name: "States", Columns: []*spec.Column{
		{Name: "state_hash", Source: "state_hash", PrimaryKey: true, Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"state_hash,primaryKey=false"`},
		{Name: "flow_hash", Source: "flow_hash", Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"flow_hash,primaryKey=true"`},
	}}
	keys, err := canonicalKeys(view)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0].Source != "flow_hash" {
		t.Fatalf("keys=%+v", keys)
	}
	if !view.Columns[0].PrimaryKey {
		t.Fatal("database schema evidence was overwritten")
	}
}
