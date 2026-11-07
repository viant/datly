package output

import (
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

func TestOpenOrdinalOutputSchemaAndValue(t *testing.T) {
	type row struct {
		Mode *int64 `json:"mode" choice:"1:A" enumAccept:"scalar" enumInteger:"passthrough" enumUnknownString:"0"`
	}
	for _, caseFormat := range []string{"", "lc"} {
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[row](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: caseFormat}}})
		if err != nil {
			t.Fatal(err)
		}
		schema, err := plan.JSONSchema()
		if err != nil {
			t.Fatal(err)
		}
		field := schema["properties"].(map[string]any)["mode"].(map[string]any)
		if _, ok := field["enum"]; ok {
			t.Fatalf("open output restricted: %+v", field)
		}
		n := int64(9007199254740993)
		for _, value := range []row{{Mode: &n}, {}} {
			encoded, err := plan.Encode(t.Context(), "json", value)
			if err != nil {
				t.Fatal(err)
			}
			r, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewBytesLoader(encoded.Data))
			if err != nil || !r.Valid() {
				t.Fatalf("output=%s err=%v result=%v", encoded.Data, err, r)
			}
		}
	}
}
