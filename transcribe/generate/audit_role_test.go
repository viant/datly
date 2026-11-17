package generate

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestAuditRoleKeepsPersistenceAndIndependentDiffMetadata(t *testing.T) {
	for _, diff := range []string{"", ` diff:"-"`} {
		column := &spec.Column{Name: "UpdatedAt", Source: "UPDATED_AT", Tag: `writer:"audit" internal:"true"` + diff}
		got := reflect.StructTag(scalarColumnFieldTag(column, "UPDATED_AT", false))
		if got.Get("writer") != "audit" || got.Get("sqlx") != "UPDATED_AT" || got.Get("internal") != "true" {
			t.Fatalf("audit role lost persistence or visibility: %s", got)
		}
	}
	// A business field hidden from the change log must not become an audit field.
	got := reflect.StructTag(scalarColumnFieldTag(&spec.Column{Name: "Budget", Source: "BUDGET", Tag: `diff:"-"`}, "BUDGET", false))
	if got.Get("writer") != "" || got.Get("sqlx") != "BUDGET" {
		t.Fatalf("diff visibility altered write role: %s", got)
	}
}
