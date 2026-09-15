package sql

import (
	"reflect"
	"testing"
)

func TestLiteralKindsUsesResultOrdinals(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		labels    []string
		want      map[int]string
	}{
		{"string", `SELECT id,'' AS value FROM records`, []string{"id", "value"}, map[int]string{1: "string"}},
		{"int wildcard", `SELECT r.* FROM (SELECT o.*,0 AS value FROM records o) r`, []string{"id", "value"}, map[int]string{1: "int"}},
		{"literal prefix", `SELECT '' AS value,o.* FROM records o`, []string{"value", "id"}, map[int]string{0: "string"}},
		{"quoted", `SELECT r.* FROM (SELECT o.*,'' AS "a.b" FROM records o) r`, []string{"id", "a.b"}, map[int]string{1: "string"}},
		{"backtick", "SELECT r.* FROM (SELECT o.*,0 AS `value` FROM records o) r", []string{"id", "value"}, map[int]string{1: "int"}},
		{"renamed duplicate", `SELECT r.* FROM (SELECT o.*,'' AS value FROM records o) r`, []string{"id", "value", "value:1"}, nil},
		{"duplicate labels", `SELECT value,'' AS value FROM records`, []string{"value", "value"}, nil},
		{"not same ordinal", `SELECT o.*,0 AS value FROM records o`, []string{"value", "id"}, nil},
		{"wrong qualifier", `SELECT wrong.* FROM (SELECT 0 AS value FROM records) r`, []string{"value"}, nil},
		{"opaque CTE no guesses", `WITH x AS (SELECT 0 AS value FROM records) SELECT * FROM x`, []string{"value"}, nil},
		{"computed no guesses", `SELECT coalesce(value,0) AS value FROM records`, []string{"value"}, nil},
		{"set no guesses", `SELECT 0 AS value FROM records UNION SELECT value FROM records`, []string{"value"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := (SelectorProjection{SQL: tc.sql}).LiteralKinds(tc.labels)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
