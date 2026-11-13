package generate

import (
	"fmt"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"strings"
	"testing"
)

func TestCanonicalMCPFlagsAreEmittedOnce(t *testing.T) {
	falseValue, trueValue := false, true
	for _, key := range []string{"mcpEnabled", "pathMcpEnabled"} {
		for _, tc := range []struct {
			name      string
			raw       bool
			canonical *bool
			want      bool
		}{
			{"raw false", false, nil, false}, {"raw true", true, nil, true},
			{"matching false", false, &falseValue, false}, {"matching true", true, &trueValue, true},
			{"override false", true, &falseValue, false}, {"override true", false, &trueValue, true},
		} {
			t.Run(key+"/"+tc.name, func(t *testing.T) {
				p := &spec.Parameter{Name: "Id", TypeExpr: "[]int", Source: spec.BindSource{Kind: "path", Name: "id"}, Tag: fmt.Sprintf(`%s:"%t" mcp:"aliases=ids" json:"id"`, key, tc.raw)}
				if key == "mcpEnabled" {
					p.MCP = tc.canonical
				} else {
					p.PathMCP = tc.canonical
				}
				metadata, err := canonicalFieldMetadata(p)
				if err != nil {
					t.Fatal(err)
				}
				for generation := 0; generation < 2; generation++ {
					raw := fieldTag(p, "Id", metadata)
					if strings.Count(raw, key+`:"`) != 1 {
						t.Fatalf("generation %d duplicated flag: %s", generation, raw)
					}
					tag := reflect.StructTag(raw)
					if tag.Get(key) != fmt.Sprint(tc.want) || tag.Get("mcp") != "aliases=ids" || tag.Get("json") != "id" {
						t.Fatalf("lost authored metadata: %s", raw)
					}
					parsed, err := dtag.ParseField(reflect.StructField{Name: "Id", Type: reflect.TypeFor[[]int](), Tag: tag})
					if err != nil {
						t.Fatal(err)
					}
					actual := parsed.MCP
					if key == "pathMcpEnabled" {
						actual = parsed.PathMCP
					}
					if actual == nil || *actual != tc.want {
						t.Fatalf("incorrect reloaded flag: %+v", parsed)
					}
					p.Tag = raw
				}
			})
		}
	}
}

func TestCanonicalMCPFlagsCoexist(t *testing.T) {
	enabled, pathEnabled := true, false
	p := &spec.Parameter{Name: "Id", TypeExpr: "[]int", Source: spec.BindSource{Kind: "path", Name: "id"}, MCP: &enabled, PathMCP: &pathEnabled, Tag: `mcpEnabled:"false" pathMcpEnabled:"true" mcp:"aliases=ids"`}
	metadata, err := canonicalFieldMetadata(p)
	if err != nil {
		t.Fatal(err)
	}
	raw := fieldTag(p, "Id", metadata)
	parsed, err := dtag.ParseField(reflect.StructField{Name: "Id", Type: reflect.TypeFor[[]int](), Tag: reflect.StructTag(raw)})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.MCP == nil || !*parsed.MCP || parsed.PathMCP == nil || *parsed.PathMCP || strings.Count(raw, `mcpEnabled:"`) != 1 || strings.Count(raw, `pathMcpEnabled:"`) != 1 {
		t.Fatalf("independent flags lost: %s", raw)
	}
}
