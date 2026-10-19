package tool

import (
	"reflect"
	"testing"
)

func TestLegacyNamedSchemaRequiredness(t *testing.T) {
	for _, tc := range []struct {
		name, kind string
		uri        bool
		typ        reflect.Type
		tag        string
		want       bool
	}{
		{"required query remains advertised optional", "query", false, reflect.TypeFor[int](), `parameter:",required=true"`, false},
		{"required form remains advertised optional", "form", false, reflect.TypeFor[int](), "", false},
		{"URI scalar", "path", true, reflect.TypeFor[int](), "", true},
		{"URI pointer", "path", true, reflect.TypeFor[*int](), "", false},
		{"unscoped path", "path", false, reflect.TypeFor[int](), "", false},
		{"body scalar", "body", false, reflect.TypeFor[int](), "", false},
		{"body slice", "body", false, reflect.TypeFor[[]int](), "", false},
		{"body optional", "body", false, reflect.TypeFor[struct{ ID int }](), `optional:"true"`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := legacyNamedSchemaRequired(tc.kind, tc.uri, tc.typ, tc.tag); got != tc.want {
				t.Fatalf("got %t want %t", got, tc.want)
			}
		})
	}
}
