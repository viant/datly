package output

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	xshape "github.com/viant/x/shape"
)

type configuredStandardJSON struct{}

func (*configuredStandardJSON) Marshal(value any) ([]byte, error) { return json.Marshal(value) }

type configuredStructologyJSON struct{}

func (*configuredStructologyJSON) Marshal(value any) ([]byte, error) {
	return structjson.Marshal(value)
}

func TestEffectiveOutputPreservesConfiguredEncoder(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[configuredStandardJSON](), reflect.TypeFor[configuredStructologyJSON]()} {
		original := reflect.TypeFor[effectiveOutput]()
		base, err := (Compiler{Lookup: func(string) (reflect.Type, error) { return typ, nil }}).Compile(CompileInput{Type: original, Component: &spec.Component{Settings: &spec.Settings{JSONMarshalType: "configured"}}})
		if err != nil {
			t.Fatal(err)
		}
		walk, _ := xshape.CompileRewrite(original)
		effective, err := walk.Rewrite(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
			if f.Name == "Secret" {
				return &xshape.FieldMetadata{Name: f.Name, Tag: `json:"-"`}, nil
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		plan, err := base.WithType(effective)
		if err != nil {
			t.Fatal(err)
		}
		if reflect.TypeOf(plan.custom) != reflect.PointerTo(typ) {
			t.Fatal("configured encoder replaced")
		}
		result, err := plan.Encode(t.Context(), "json", &effectiveOutput{Public: "visible", Secret: "private", Nested: []*effectiveNested{{Secret: "private child", Visible: "child"}}})
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err = json.Unmarshal(result.Data, &decoded); err != nil {
			t.Fatal(err)
		}
		if _, found := decoded["Secret"]; found {
			t.Fatal("configured encoder received original representation")
		}
		child := decoded["Nested"].([]any)[0].(map[string]any)
		if _, found := child["Secret"]; found {
			t.Fatal("nested configured encoding bypassed policy")
		}
	}
}

type effectiveNested struct {
	Secret  string
	Visible string
}
type effectiveOutput struct {
	Secret string
	Nested []*effectiveNested
	Public string
}

func TestEffectiveOutputUsesProjectedValuesWithNativeJSON(t *testing.T) {
	for _, casing := range []string{"", "lc"} {
		original := reflect.TypeFor[effectiveOutput]()
		base, err := (Compiler{}).Compile(CompileInput{Type: original, Component: &spec.Component{Settings: &spec.Settings{CaseFormat: casing}}})
		if err != nil {
			t.Fatal(err)
		}
		walk, _ := xshape.CompileRewrite(original)
		effective, err := walk.Rewrite(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
			if f.Name == "Secret" {
				return &xshape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		plan, err := base.WithType(effective)
		if err != nil {
			t.Fatal(err)
		}
		value := &effectiveOutput{Secret: "private root", Nested: []*effectiveNested{{Secret: "private nested", Visible: "visible"}}, Public: "public"}
		encoded, err := plan.Encode(context.Background(), "json", value)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err = json.Unmarshal(encoded.Data, &decoded); err != nil {
			t.Fatal(err)
		}
		name, nested, secret := "Public", "Nested", "Secret"
		if casing != "" {
			name, nested, secret = "public", "nested", "secret"
		}
		if decoded[name] != "public" {
			t.Fatalf("%s", encoded.Data)
		}
		if _, ok := decoded[secret]; ok {
			t.Fatal("root secret leaked")
		}
		if _, ok := decoded[nested].([]any)[0].(map[string]any)[secret]; ok {
			t.Fatal("nested secret leaked")
		}
		if value.Secret != "private root" || value.Nested[0].Secret != "private nested" {
			t.Fatal("canonical output mutated")
		}
		if _, err = plan.Encode(context.Background(), "xml", value); err == nil {
			t.Fatal("unsupported format bypassed policy")
		}
		if noChange, err := base.WithType(original); err != nil || noChange != base {
			t.Fatal("no-op replaced original plan")
		}
	}
}
