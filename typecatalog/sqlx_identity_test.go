package typecatalog

import (
	"reflect"
	"testing"
)

func TestSQLXPrimaryKeyMappingAuthority(t *testing.T) {
	type useCase struct {
		desc             string
		tag              reflect.StructTag
		inferred, expect bool
	}
	for _, tc := range []useCase{
		{"discovery fallback", `sqlx:"id"`, true, true},
		{"bare declared identity", `sqlx:"id,primaryKey"`, false, true},
		{"declared true", `sqlx:"id,primaryKey=true"`, false, true},
		{"declared false overrides physical discovery", `sqlx:"id,primaryKey=false"`, true, false},
		{"column name is not an option", `sqlx:"primarykey_note"`, false, false},
		{"transient fields cannot identify DML", `sqlx:"-"`, true, false},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if got := SQLXPrimaryKey(tc.tag, tc.inferred); got != tc.expect {
				t.Fatalf("got=%v expected=%v", got, tc.expect)
			}
		})
	}
}
