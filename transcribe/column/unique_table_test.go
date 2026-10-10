package column

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/validator"
)

func TestUniqueValidationTableUsesPhysicalLineage(t *testing.T) {
	for _, tc := range []struct{ name, sql, tag, table, want string }{
		{"alias", "SELECT r.code AS public_code FROM records r", "", "records", "records"},
		{"qualified", "SELECT r.code AS public_code FROM archive.records r", "", "archive.records", "archive.records"},
		{"authored table", "SELECT r.code AS public_code FROM records r", `sqlx:"code,table=chosen"`, "records", "chosen"},
		{"authored disabled", "SELECT r.code AS public_code FROM records r", `sqlx:"code,unique=false"`, "records", ""},
		{"authored empty table", "SELECT r.code AS public_code FROM records r", `sqlx:"code,table="`, "records", ""},
		{"authored composite", "SELECT r.code AS public_code FROM records r", `sqlx:"code,uniqueDep=tenant"`, "records", ""},
		{"transient", "SELECT r.code AS public_code FROM records r", `sqlx:"-"`, "records", ""},
		{"computed", "SELECT r.code || 'x' AS public_code FROM records r", "", "records", ""},
		{"wrong source", "SELECT r.code AS public_code FROM other r", "", "records", ""},
		{"ambiguous", "SELECT code AS public_code FROM records r JOIN other o ON o.id=r.id", "", "records", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lineage, err := directProjectionLineage(&spec.ViewSource{Table: tc.table, SQL: tc.sql})
			if err != nil {
				t.Fatal(err)
			}
			col := &spec.Column{Name: "public_code", Source: "public_code", Tag: tc.tag}
			applyTableConstraints([]*spec.Column{col}, map[string]tableConstraint{"code": {name: "code", unique: true}}, lineage, tc.table)
			meta := sqlio.ParseTag(reflect.StructTag(col.Tag))
			if meta.Table != tc.want {
				t.Fatalf("table=%q want %q: %+v", meta.Table, tc.want, col)
			}
			first := col.Tag
			applyTableConstraints([]*spec.Column{col}, map[string]tableConstraint{"code": {name: "code", unique: true}}, lineage, tc.table)
			if col.Tag != first {
				t.Fatalf("unstable tag %q -> %q", first, col.Tag)
			}
			if tc.name == "alias" || tc.name == "qualified" {
				if col.Source != "code" || !strings.HasPrefix(meta.Column, "code") || !strings.Contains(col.Tag, "code|public_code") {
					t.Fatalf("physical/result identities lost: %+v", col)
				}
				row := reflect.StructOf([]reflect.StructField{{Name: "Code", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(strings.Replace(col.Tag, `,table=`, `,unique=true,table=`, 1))}})
				checks, err := validator.NewChecks(row, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(checks.Unique) != 1 || !strings.Contains(checks.Unique[0].SQL, "FROM "+tc.want+" WHERE code") {
					t.Fatalf("native unique check absent: %+v", checks.Unique)
				}
			}
		})
	}
}
