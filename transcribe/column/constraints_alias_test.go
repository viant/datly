package column

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
)

func TestInferredForeignKeyRetainsProvenResultAlias(t *testing.T) {
	for _, tc := range []struct {
		name, sql, nameInShape, source, tag, want string
		notNull                                   bool
	}{
		{"nullable alias", "SELECT c.PARENT_ID AS PARENT FROM child c", "PARENT", "PARENT", "", "PARENT_ID|PARENT", false},
		{"required alias", "SELECT c.PARENT_ID AS PARENT FROM child c", "PARENT", "PARENT", "", "PARENT_ID|PARENT", true},
		{"unaliased", "SELECT c.PARENT_ID FROM child c", "PARENT_ID", "PARENT_ID", "", "PARENT_ID", true},
		{"case-equivalent", "SELECT c.PARENT_ID AS parent_id FROM child c", "parent_id", "parent_id", "", "PARENT_ID", false},
		{"outer compiler mapping", "SELECT c.PARENT_ID FROM child c", "CustomName", "PARENT_ID", `sqlx:"PARENT_ID"`, "PARENT_ID", false},
		{"inner plus outer", "SELECT c.PARENT_ID AS PARENT FROM child c", "CustomName", "PARENT", `sqlx:"PARENT"`, "PARENT", false},
		{"authored alternatives", "SELECT c.PARENT_ID AS PARENT FROM child c", "PARENT", "PARENT", `sqlx:"CUSTOM|SECOND,required=false,enc=JSON,refDb=authored_db,refTable=authored_table,refColumn=authored_id"`, "CUSTOM|SECOND", true},
		{"authored name option", "SELECT c.PARENT_ID AS PARENT FROM child c", "PARENT", "PARENT", `sqlx:"name=CUSTOM,required=false"`, "name=CUSTOM", true},
		{"transient", "SELECT c.PARENT_ID AS PARENT FROM child c", "PARENT", "PARENT", `sqlx:"-"`, "-", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lineage, err := directProjectionLineage(&spec.ViewSource{Table: "child", SQL: tc.sql})
			if err != nil {
				t.Fatal(err)
			}
			column := &spec.Column{Name: tc.nameInShape, Source: tc.source, Tag: tc.tag, Nullable: !tc.notNull}
			def := "7"
			constraints := map[string]tableConstraint{"parent_id": {name: "PARENT_ID", notNull: tc.notNull, unique: true, defaultValue: &def, reference: &tableReference{schema: "remote", table: "parent", column: "ID"}}}
			applyTableConstraints([]*spec.Column{column}, constraints, lineage)
			raw := reflect.StructTag(column.Tag).Get("sqlx")
			mapping := strings.Split(raw, ",")[0]
			if mapping != tc.want {
				t.Fatalf("mapping=%q want %q: %s", mapping, tc.want, column.Tag)
			}
			first := column.Tag
			applyTableConstraints([]*spec.Column{column}, constraints, lineage)
			if first != column.Tag {
				t.Fatalf("repeat refinement changed mapping: %q -> %q", first, column.Tag)
			}
			parsed := sqlio.ParseTag(reflect.StructTag(column.Tag))
			if tc.name == "authored alternatives" {
				if raw != `CUSTOM|SECOND,required=false,enc=JSON,refDb=authored_db,refTable=authored_table,refColumn=authored_id` {
					t.Fatalf("authored mapping/options changed: %s", raw)
				}
			} else if parsed.RefDb != "remote" || parsed.RefTable != "parent" || parsed.RefColumn != "ID" {
				t.Fatalf("FK provenance lost: %+v", parsed)
			}
			if column.NotNull != tc.notNull || column.Nullable != !tc.notNull || !column.Unique || column.Default == nil || *column.Default != def {
				t.Fatalf("physical facts changed: %+v", column)
			}
		})
	}
}

func TestInferredForeignKeyDoesNotInventResultLineage(t *testing.T) {
	for _, sql := range []string{
		"SELECT c.PARENT_ID+1 AS PARENT FROM child c",
		"SELECT PARENT_ID AS PARENT FROM child c JOIN other o ON o.ID=c.PARENT_ID",
		"SELECT o.PARENT_ID AS PARENT FROM child c JOIN other o ON o.ID=c.PARENT_ID",
	} {
		lineage, err := directProjectionLineage(&spec.ViewSource{Table: "child", SQL: sql})
		if err != nil {
			t.Fatal(err)
		}
		column := &spec.Column{Name: "PARENT", Source: "PARENT"}
		applyTableConstraints([]*spec.Column{column}, map[string]tableConstraint{"parent_id": {name: "PARENT_ID", reference: &tableReference{table: "parent", column: "ID"}}}, lineage)
		if column.Tag != "" || column.Source != "PARENT" {
			t.Fatalf("invented physical/result metadata for %q: %+v", sql, column)
		}
	}
	lineage, err := directProjectionLineage(&spec.ViewSource{Table: "child", SQL: "SELECT c.PARENT_ID AS PARENT FROM child c"})
	if err != nil {
		t.Fatal(err)
	}
	column := &spec.Column{Name: "PARENT", Source: "PARENT"}
	applyTableConstraints([]*spec.Column{column}, map[string]tableConstraint{"parent_id": {name: "PARENT_ID", notNull: true}}, lineage)
	if column.Tag != "" || column.Source != "PARENT_ID" || !column.NotNull {
		t.Fatalf("no-FK enrichment changed: %+v", column)
	}
}
