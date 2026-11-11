package openapi

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/gateway/openapi/openapi3"
)

func TestObjectToSliceSchemaRetainsTypedItems(t *testing.T) {
	type record struct {
		CreativeID *int   `json:"creativeId"`
		Hidden     string `internal:"true"`
	}
	type input struct {
		Pairs []record `json:"pairs" jsonx:"objectToSlice"`
	}
	schemas := openapi3.Schemas{}
	ref, err := newSchemaBuilder(schemas).schema(reflect.TypeFor[input](), true)
	if err != nil {
		t.Fatal(err)
	}
	property := schemas[strings.TrimPrefix(ref.Ref, "#/components/schemas/")].Properties["pairs"]
	var object, array *openapi3.Schema
	for _, branch := range property.AnyOf {
		if branch.Type == "array" {
			array = branch
		} else if branch.Type == "object" || branch.Ref != "" {
			object = branch
		}
	}
	if object == nil || array == nil || array.Items == nil {
		t.Fatalf("missing typed object/array alternatives: %+v", property)
	}
	resolve := func(s *openapi3.Schema) *openapi3.Schema {
		if len(s.AnyOf) > 0 {
			s = s.AnyOf[0]
		}
		if s.Ref != "" {
			return schemas[strings.TrimPrefix(s.Ref, "#/components/schemas/")]
		}
		return s
	}
	for _, branch := range []*openapi3.Schema{resolve(object), resolve(array.Items)} {
		if branch == nil || branch.Properties["creativeId"] == nil || branch.Properties["hidden"] != nil {
			t.Fatalf("record graph lost or internal field exposed: %+v", branch)
		}
	}
}
