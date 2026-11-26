package contract

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	structjson "github.com/viant/structology/encoding/json"
	xcontract "github.com/viant/xdatly/contract"
)

type mcpNamedRecord struct {
	Model int `json:"pricingModel" mcp:"name=LegacyName,aliases=legacy|old"`
}

func (*mcpNamedRecord) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "pricing-name:r1", Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[mcpNamedRecord]() && f.Name == "Model" {
			return f.SetMCPName("PricingModel"), nil
		}
		return nil, nil
	}}, nil
}
func TestMCPNameHelperPreservesJSONAndAliases(t *testing.T) {
	typ := reflect.TypeFor[mcpNamedRecord]()
	plan, err := New(typ, 8)
	if err != nil {
		t.Fatal(err)
	}
	binder, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := plan.ResolveRepresentation(t.Context(), binder)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := (jsonschema.Reflector{}).Compile(jsonschema.Request{Type: typ, Representation: metadata})
	if err != nil {
		t.Fatal(err)
	}
	properties := schema["properties"].(map[string]any)
	if properties["PricingModel"] == nil || properties["LegacyName"] != nil {
		t.Fatalf("%+v", properties)
	}
	for _, name := range []string{"PricingModel", "legacy", "old", "pricingModel"} {
		normalized, err := fieldname.Normalize(typ, map[string]any{name: 10.0}, fieldname.Visibility{Representation: metadata})
		if err != nil || normalized.(map[string]any)["pricingModel"] != 10.0 {
			t.Fatalf("%s: %+v %v", name, normalized, err)
		}
	}
	raw, err := structjson.Marshal(mcpNamedRecord{Model: 10}, structjson.WithRepresentation(metadata))
	if err != nil || string(raw) != `{"pricingModel":10}` {
		t.Fatalf("JSON name changed: %s %v", raw, err)
	}
	if metadata.Field(0).Name != "Model" {
		t.Fatal("Go metadata name changed")
	}
	again, err := plan.ResolveRepresentation(t.Context(), binder)
	if err != nil || again != metadata {
		t.Fatal("named variant not reused", err)
	}
}
