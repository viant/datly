package sql

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
	"testing/fstest"
)

func TestResolveSourceValidatesInlineAndResourceStructure(t *testing.T) {
	for _, tc := range []struct {
		name, sql      string
		resource, fail bool
	}{
		{"inline invalid", "SELECT (", false, true},
		{"resource invalid", "SELECT (", true, true},
		{"inline quoted", "SELECT '(' AS value", false, false},
		{"resource comment", "SELECT 1 /* ( */", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &spec.ViewSource{SQL: tc.sql}
			if tc.resource {
				source = &spec.ViewSource{URI: "query.sql"}
			}
			before := source.Clone()
			err := ResolveSource("records", source, fstest.MapFS{"query.sql": {Data: []byte(tc.sql)}})
			if (err != nil) != tc.fail {
				t.Fatalf("err=%v", err)
			}
			if tc.fail && !reflect.DeepEqual(source, before) {
				t.Fatal("invalid resource partially published")
			}
		})
	}
}
