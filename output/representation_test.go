package output

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/x/shape"
)

type RepresentationEmbedded struct {
	Public string `json:"public"`
}

func (*RepresentationEmbedded) Identity() string { return "linked" }

type representationOutput struct {
	*RepresentationEmbedded
	Value  int    `json:"value"`
	Secret string `json:"secret"`
}
type representationAwareEncoder struct{}

func (*representationAwareEncoder) Marshal(value any) ([]byte, error) { return json.Marshal(value) }
func (*representationAwareEncoder) MarshalWithRepresentation(value any, metadata *shape.Representation) ([]byte, error) {
	return structjson.MarshalStandard(value, structjson.WithRepresentation(metadata))
}
func TestOutputRepresentationPreservesLinkedEmbeddingAndMetadata(t *testing.T) {
	original := reflect.TypeFor[representationOutput]()
	walker, _ := shape.CompileRewrite(original)
	effective, err := walker.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		switch f.Name {
		case "Secret":
			return &shape.FieldMetadata{Name: f.Name, Tag: `json:"-"`}, nil
		case "Value":
			return &shape.FieldMetadata{Name: "Renamed", Tag: `json:"visible" required:"true" choice:"0" choice:"7" description:"Visible"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"standard", "formatted", "configured"} {
		t.Run(mode, func(t *testing.T) {
			settings := &spec.Settings{}
			if mode == "formatted" {
				settings.CaseFormat = "lc"
			}
			if mode == "configured" {
				settings.JSONMarshalType = "configured"
			}
			component := &spec.Component{Settings: settings}
			base, err := (Compiler{Lookup: func(string) (reflect.Type, error) { return reflect.TypeFor[representationAwareEncoder](), nil }}).Compile(CompileInput{Type: original, Component: component})
			if err != nil {
				t.Fatal(err)
			}
			// Recompilation must use the captured registration, not mutable source settings.
			component.Settings = &spec.Settings{CaseFormat: "uc"}
			plan, err := base.WithRepresentation(effective)
			if err != nil {
				t.Fatal(err)
			}
			value := &representationOutput{RepresentationEmbedded: &RepresentationEmbedded{Public: "linked"}, Value: 7, Secret: "private"}
			encoded, err := plan.Encode(t.Context(), "json", value)
			if err != nil {
				t.Fatal(err)
			}
			var object map[string]any
			if err = json.Unmarshal(encoded.Data, &object); err != nil {
				t.Fatal(err)
			}
			if len(object) != 2 || object["public"] != "linked" || object["visible"] != float64(7) {
				t.Fatalf("%s", encoded.Data)
			}
			if value.Secret != "private" || value.Identity() != "linked" || plan.typeOf != original {
				t.Fatal("canonical linked value was changed")
			}
			if mode != "configured" {
				schema, err := plan.JSONSchema()
				if err != nil {
					t.Fatal(err)
				}
				properties := schema["properties"].(map[string]any)
				if properties["secret"] != nil || properties["visible"] == nil || !reflect.DeepEqual(schema["required"], []string{"visible"}) {
					t.Fatal(schema)
				}
				wire, err := plan.Wire("json")
				if err != nil {
					t.Fatal(err)
				}
				if wire.JSON.Representation() != effective {
					t.Fatal("wire metadata differs from encoding")
				}
			}
			if same, err := base.WithRepresentation(nil); err != nil || same != base {
				t.Fatal("nil metadata changed plan")
			}
			plain, err := base.Encode(t.Context(), "json", value)
			if err != nil {
				t.Fatal(err)
			}
			var canonical map[string]any
			_ = json.Unmarshal(plain.Data, &canonical)
			if canonical["secret"] != "private" {
				t.Fatal("base encoder contaminated")
			}
		})
	}
}
