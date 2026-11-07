package openapi

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/gateway/openapi/openapi3"
	jsonmarshal "github.com/viant/structology/encoding/json/marshal"
	"github.com/viant/x/shape"
)

type RepresentationEmbedded struct {
	Public string `json:"public"`
}

func (*RepresentationEmbedded) Identity() string { return "linked" }

type representationBody struct {
	*RepresentationEmbedded
	Mode   string `json:"mode"`
	Secret string `json:"secret"`
}

func TestOpenAPIMetadataSupportsMethodBearingEmbedding(t *testing.T) {
	original := reflect.TypeFor[representationBody]()
	walker, _ := shape.CompileRewrite(original)
	effective, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		switch f.Name {
		case "Secret":
			return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
		case "Mode":
			return &shape.FieldMetadata{Name: "Workflow", Tag: `json:"workflow" required:"true" choice:"one" choice:"two" description:"Caller mode" example:"one"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := effective.Reflected(); err == nil {
		t.Fatal("fixture must exercise the reflection restriction")
	}
	for _, input := range []bool{false, true} {
		schemas := newSchemaBuilder(openapi3.Schemas{})
		// All variants use the same native type/path, but must not share a definition.
		base, err := schemas.schema(original, input)
		if err != nil {
			t.Fatal(err)
		}
		caller, err := schemas.schemaRepresentation(original, input, effective)
		if err != nil {
			t.Fatal(err)
		}
		if caller.Ref == base.Ref {
			t.Fatal("caller metadata shared base schema identity")
		}
		object := schemas.schemas[strings.TrimPrefix(caller.Ref, "#/components/schemas/")]
		if len(object.Properties) != 2 || object.Properties["public"] == nil || object.Properties["workflow"] == nil {
			t.Fatalf("embedding/field shape lost: %+v", object)
		}
		mode := object.Properties["workflow"]
		if mode.Description != "Caller mode" || mode.Example != "one" || !reflect.DeepEqual(mode.Enum, []any{"one", "two"}) || !reflect.DeepEqual(object.Required, []string{"workflow"}) {
			t.Fatalf("metadata lost: %+v", mode)
		}
		originalObject := schemas.schemas[strings.TrimPrefix(base.Ref, "#/components/schemas/")]
		if originalObject.Properties["secret"] == nil || originalObject.Properties["mode"] == nil || originalObject.Properties["workflow"] != nil {
			t.Fatal("original schema mutated")
		}
		linked, _ := walker.RewriteRepresentation(nil)
		unchanged, err := schemas.schemaRepresentation(original, input, linked)
		if err != nil || unchanged.Ref != base.Ref {
			t.Fatal("opt-in metadata altered no-op schema", err)
		}
	}
}

func TestOpenAPISerializerMetadataSupportsLinkedEmbedding(t *testing.T) {
	original := reflect.TypeFor[representationBody]()
	walker, _ := shape.CompileRewrite(original)
	effective, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if f.Name == "Secret" {
			return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	encoder := jsonmarshal.NewStandard(original)
	base, err := encoder.Wire()
	if err != nil {
		t.Fatal(err)
	}
	selected, err := encoder.WithRepresentation(effective).Wire()
	if err != nil {
		t.Fatal(err)
	}
	builder := newSchemaBuilder(openapi3.Schemas{})
	plain, err := builder.wire(base)
	if err != nil {
		t.Fatal(err)
	}
	caller, err := builder.wire(selected)
	if err != nil {
		t.Fatal(err)
	}
	if plain.Ref == caller.Ref {
		t.Fatal("wire schema variants share identity")
	}
	object := builder.schemas[strings.TrimPrefix(caller.Ref, "#/components/schemas/")]
	if object.Properties["secret"] != nil || object.Properties["public"] == nil || object.Properties["mode"] == nil {
		t.Fatal("wire metadata does not match effective encoding")
	}
}

func TestOpenAPIInputRepresentationSupportsIntegerMapComposite(t *testing.T) {
	type group struct {
		Rows [][]*representationBody       `json:"rows"`
		ByID map[int][]*representationBody `json:"byId"`
	}
	type root struct {
		Groups []*group `json:"groups"`
	}
	original := reflect.TypeFor[root]()
	walker, _ := shape.CompileRewrite(original)
	metadata, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[representationBody]() && f.Name == "Secret" {
			return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	builder := newSchemaBuilder(openapi3.Schemas{})
	selected, err := builder.schemaRepresentation(original, true, metadata)
	if err != nil {
		t.Fatal(err)
	}
	deref := func(value *openapi3.Schema) *openapi3.Schema {
		if len(value.AnyOf) > 0 {
			value = value.AnyOf[0]
		}
		if value.Ref != "" {
			return builder.schemas[strings.TrimPrefix(value.Ref, "#/components/schemas/")]
		}
		return value
	}
	object := deref(selected)
	grouped := deref(object.Properties["groups"].Items)
	mapping := grouped.Properties["byId"]
	if mapping.Type != "object" || mapping.AdditionalProperties == nil || mapping.AdditionalProperties.Type != "array" {
		t.Fatal("map composite shape lost")
	}
	leaf := deref(mapping.AdditionalProperties.Items)
	if leaf.Properties["secret"] != nil || leaf.Properties["public"] == nil {
		t.Fatal("nested embedded visibility lost")
	}
}
