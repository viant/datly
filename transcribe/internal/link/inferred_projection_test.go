package link

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestInferredProjectionOverlay(t *testing.T) {
	for _, tc := range []struct {
		name, kind, sql, typ, output, baseTag, tag string
		inferred                                   bool
	}{
		{name: "inferred", kind: "param", sql: "SELECT ARRAY_AGG(Id) AS Values FROM `/`", baseTag: `json:"ids"`, inferred: true},
		{name: "explicit", kind: "param", sql: "SELECT Id FROM `/`", typ: "Explicit"},
		{name: "explicit output", kind: "param", sql: "SELECT Id FROM `/`", output: "Explicit"},
		{name: "inherited type name", kind: "param", sql: "SELECT Id FROM `/`", baseTag: `typeName:"Explicit"`},
		{name: "authored type name", kind: "param", sql: "SELECT Id FROM `/`", tag: `typeName:"Explicit"`},
		{name: "no SQL", kind: "param"},
		{name: "body", kind: "body", sql: "SELECT Id FROM `/`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := &spec.Parameter{TypeExpr: "OldInput", OutputTypeExpr: "OldOutput", Tag: tc.baseTag}
			authored := &spec.Parameter{Source: spec.BindSource{Kind: tc.kind}, DeclarationSQL: tc.sql, TypeExpr: tc.typ, OutputTypeExpr: tc.output, Tag: tc.tag}
			got := overlayParam(base, authored)
			input, output := "OldInput", "OldOutput"
			if tc.typ != "" {
				input = tc.typ
			}
			if tc.output != "" {
				output = tc.output
			}
			if tc.inferred {
				input, output = "", ""
			}
			if got.TypeExpr != input || got.OutputTypeExpr != output {
				t.Fatalf("types = %q, %q; want %q, %q", got.TypeExpr, got.OutputTypeExpr, input, output)
			}
			tag := tc.baseTag
			if tc.tag != "" {
				tag = tc.tag
			}
			if got.Tag != tag {
				t.Fatalf("tag changed: %q", got.Tag)
			}
			if base.TypeExpr != "OldInput" || base.OutputTypeExpr != "OldOutput" {
				t.Fatal("mutated base")
			}
		})
	}
}
