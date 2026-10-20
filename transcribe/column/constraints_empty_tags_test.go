package column

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestConstraintEnrichmentPreservesAuthoredJSONTags(t *testing.T) {
	for _, tag := range []string{`json:""`, `json:",omitempty"`, `json:"publicName"`, `json:"-"`} {
		for _, tc := range []struct {
			description  string
			apply        func(*spec.Column)
			expectedSQLX string
		}{
			{"foreign key", func(c *spec.Column) { applyReferenceConstraint(c, &tableReference{table: "parents", column: "ID"}, "") }, "PARENT_ID,refTable=parents,refColumn=ID"},
			{"unique", func(c *spec.Column) { applyUniqueTableConstraint(c, "children", "") }, "PARENT_ID,table=children"},
			{"physical alias", func(c *spec.Column) {
				applyTableConstraints([]*spec.Column{c}, map[string]tableConstraint{"parent_id": {}}, &projectionLineage{direct: map[string]string{"parent_id": "parent_id"}, names: map[string]string{"parent_id": "parent_alias"}})
			}, "PARENT_ID|parent_alias"},
		} {
			t.Run(tc.description+"/"+tag, func(t *testing.T) {
				c := &spec.Column{Name: "PARENT_ID", Source: "PARENT_ID", Tag: tag}
				tc.apply(c)
				expected, _ := reflect.StructTag(tag).Lookup("json")
				actual, present := reflect.StructTag(c.Tag).Lookup("json")
				if !present || actual != expected {
					t.Fatalf("authored JSON lost: %q", c.Tag)
				}
				if got := reflect.StructTag(c.Tag).Get("sqlx"); got != tc.expectedSQLX {
					t.Fatalf("SQLX=%q want %q", got, tc.expectedSQLX)
				}
			})
		}
	}
}
