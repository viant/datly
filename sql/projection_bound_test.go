package sql

import (
	"reflect"
	"testing"
)

func TestBoundTypesRequireExactResultAndBindingOrdinals(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		labels      []string
		args        []any
		want        map[int]reflect.Type
	}{
		{"scalar", `SELECT ? AS value FROM records`, []string{"value"}, []any{"constant"}, map[int]reflect.Type{0: reflect.TypeFor[string]()}},
		{"wildcard and WHERE", `SELECT r.* FROM (SELECT o.*,? AS value FROM records o WHERE id > ?) r`, []string{"id", "value"}, []any{7, "filter"}, map[int]reflect.Type{1: reflect.TypeFor[int]()}},
		{"prefix", `SELECT ? AS value,o.* FROM records o`, []string{"value", "id"}, []any{true}, map[int]reflect.Type{0: reflect.TypeFor[bool]()}},
		{"protected question marks", `SELECT '?' AS literal, ? AS value FROM records /* ? */ WHERE note = '?' AND id > ?`, []string{"literal", "value"}, []any{"constant", 1}, map[int]reflect.Type{1: reflect.TypeFor[string]()}},
		{"unparsed operator is conservative", `SELECT ? AS value FROM records WHERE payload ? 'key'`, []string{"value"}, []any{7}, nil},
		{"duplicate", `SELECT ? AS value,? AS value FROM records`, []string{"value", "value"}, []any{7, "other"}, nil},
		{"wrong ordinal", `SELECT o.*,? AS value FROM records o`, []string{"value", "id"}, []any{7}, nil},
		{"nil", `SELECT ? AS value FROM records`, []string{"value"}, []any{nil}, nil},
		{"computed", `SELECT coalesce(?,0) AS value FROM records`, []string{"value"}, []any{7}, nil},
		{"missing arg", `SELECT ? AS value FROM records WHERE id > ?`, []string{"value"}, []any{7}, nil},
		{"extra arg", `SELECT ? AS value FROM records`, []string{"value"}, []any{7, 8}, nil},
		{"CTE", `WITH x AS (SELECT ? AS value FROM records) SELECT * FROM x`, []string{"value"}, []any{7}, nil},
		{"union", `SELECT ? AS value FROM records UNION SELECT value FROM records`, []string{"value"}, []any{7}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := (SelectorProjection{SQL: tc.query}).BoundTypes(tc.labels, tc.args)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
