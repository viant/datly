package column

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/metadata/sink"
)

func TestCompositeForeignKeyRetainsCompleteMemberGroup(t *testing.T) {
	facts := map[string]tableConstraint{"scope_id": {}, "parent_id": {}, "owner_id": {}}
	keys := []sink.Key{
		{Name: "fk_target", Position: 2, Column: "parent_id", ReferenceTable: "targets", ReferenceColumn: "parent_id"},
		{Name: "fk_target", Position: 1, Column: "scope_id", ReferenceTable: "targets", ReferenceColumn: "scope_id"},
		{Name: "fk_owner", Position: 1, Column: "owner_id", ReferenceTable: "owners", ReferenceColumn: "id"},
	}
	applyForeignKeyConstraints(facts, keys, "", "children")
	for _, name := range []string{"scope_id", "parent_id", "owner_id"} {
		c := &spec.Column{Name: name}
		applyTableConstraints([]*spec.Column{c}, facts, &projectionLineage{wildcard: true})
		got := reflect.StructTag(c.Tag).Get("refKey")
		want := "scope_id,parent_id"
		if name == "owner_id" {
			want = ""
		}
		if got != want {
			t.Fatalf("%s refKey=%q want%q (tag%s)", name, got, want, c.Tag)
		}
	}
}

func TestDiscoveredCompositeForeignKeyRetainsMemberGroup(t *testing.T) {
	h := sqlite.New(t)
	for _, statement := range []string{
		"CREATE TABLE targets(scope_id TEXT NOT NULL,parent_id TEXT NOT NULL,PRIMARY KEY(scope_id,parent_id))",
		"CREATE TABLE children(id INTEGER PRIMARY KEY,scope_id TEXT NOT NULL,parent_id TEXT,FOREIGN KEY(scope_id,parent_id) REFERENCES targets(scope_id,parent_id))",
	} {
		if _, err := h.DB.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	facts, err := loadTableConstraints(context.Background(), h.DB, "children")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"scope_id", "parent_id"} {
		c := &spec.Column{Name: name}
		applyTableConstraints([]*spec.Column{c}, facts, &projectionLineage{wildcard: true})
		if got := reflect.StructTag(c.Tag).Get("refKey"); got != "scope_id,parent_id" {
			t.Fatalf("%s refKey=%q tag=%s", name, got, c.Tag)
		}
	}
}

func TestMatchFullDoesNotAcquireSimpleNullSemantics(t *testing.T) {
	facts := map[string]tableConstraint{"scope_id": {}, "parent_id": {}}
	keys := []sink.Key{{Name: "fk_target", Position: 1, Column: "scope_id", ReferenceTable: "targets", ReferenceColumn: "scope_id", OnMatch: "FULL"}, {Name: "fk_target", Position: 2, Column: "parent_id", ReferenceTable: "targets", ReferenceColumn: "parent_id", OnMatch: "FULL"}}
	applyForeignKeyConstraints(facts, keys, "", "children")
	c := &spec.Column{Name: "scope_id"}
	applyTableConstraints([]*spec.Column{c}, facts, &projectionLineage{wildcard: true})
	if got := reflect.StructTag(c.Tag).Get("refKey"); got != "" {
		t.Fatalf("MATCH FULL incorrectly assigned simple null group: %s", c.Tag)
	}
}
