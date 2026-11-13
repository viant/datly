package openapi

import (
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

func TestEffectiveFieldEnumsAcrossOpenAPIRepresentations(t *testing.T) {
	type record struct {
		Mode     *int64   `json:"mode" choice:"9007199254740993" choice:"2" required:"true"`
		Label    string   `json:"label" choice:"a" choice:"b" description:"Caller-visible label" example:"a"`
		Channels []string `json:"channels" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO"`
	}
	for _, mode := range []string{"input", "plain output", "formatted output"} {
		t.Run(mode, func(t *testing.T) {
			b := newSchemaBuilder(openapi3.Schemas{})
			var root *openapi3.Schema
			var err error
			if mode == "input" {
				root, err = b.schema(reflect.TypeFor[record](), true)
			} else {
				settings := &spec.Settings{}
				if mode == "formatted output" {
					settings.CaseFormat = "lc"
				}
				plan, e := (output.Compiler{}).Compile(output.CompileInput{Type: reflect.TypeFor[record](), Component: &spec.Component{Settings: settings}})
				if e != nil {
					t.Fatal(e)
				}
				wire, e := plan.Wire("json")
				if e != nil {
					t.Fatal(e)
				}
				root, err = b.wire(wire.JSON)
			}
			if err != nil {
				t.Fatal(err)
			}
			root = b.schemas[strings.TrimPrefix(root.Ref, "#/components/schemas/")]
			if root == nil {
				t.Fatal("missing object schema")
			}

			channels := root.Properties["channels"]
			if channels.Type != "array" || len(channels.Enum) != 0 || channels.Items == nil || !reflect.DeepEqual(channels.Items.Enum, []any{"DISPLAY", "VIDEO"}) {
				t.Fatalf("collection schema=%+v", channels)
			}
			if !reflect.DeepEqual(root.Properties["mode"].Enum, []any{int64(9007199254740993), int64(2), nil}) {
				t.Fatalf("enum=%v", root.Properties["mode"].Enum)
			}
			if !reflect.DeepEqual(root.Required, []string{"mode"}) {
				t.Fatal(root.Required)
			}
			if root.Properties["label"].Description != "Caller-visible label" || root.Properties["label"].Example != "a" {
				t.Fatal("effective annotations missing")
			}
			if !reflect.DeepEqual(root.Properties["label"].Enum, []any{"a", "b"}) {
				t.Fatal(root.Properties["label"].Enum)
			}
		})
	}
}
