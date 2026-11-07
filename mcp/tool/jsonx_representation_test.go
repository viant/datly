package tool

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/x/shape"
)

func TestJSONXSchemaUsesEffectiveRouteTags(t *testing.T) {
	type row struct {
		Flag *int `json:"flag"`
	}
	typ := reflect.TypeFor[row]()
	rewrite, err := shape.CompileRewrite(typ)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := rewrite.RewriteRepresentation(func(_ string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == typ && field.Name == "Flag" {
			return &shape.FieldMetadata{Name: field.Name, Tag: `json:"enabled" jsonx:"boolToInt,null=omit"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	projected, err := (&schemaProjector{nativeBody: true, representation: metadata}).argument(typ, "Body", "Body", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, enabled, _ := schemaMember(projected, []string{"enabled"})
	if enabled == nil {
		t.Fatalf("missing effective field: %+v", projected)
	}
	raw, err := json.Marshal(enabled["type"])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"boolean"`) {
		t.Fatalf("effective input policy omitted: %s", raw)
	}
	_, retired, _ := schemaMember(projected, []string{"flag"})
	if retired != nil {
		t.Fatal("original field name leaked into route schema")
	}
	plain, err := (&schemaProjector{nativeBody: true}).argument(typ, "Body", "Body", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"boolean"`) {
		t.Fatalf("route policy leaked: %s", raw)
	}
}
