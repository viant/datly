package output

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestOutputSchemaRetainsTypedChoiceTags(t *testing.T) {
	type record struct {
		Mode     *int64   `json:"mode" choice:"9007199254740993" choice:"2"`
		Label    string   `json:"label" choice:"a" choice:"b" example:"a"`
		Channels []string `json:"channels" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO"`
	}
	for _, format := range []string{"", "lc"} {
		plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[record](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: format}}})
		if err != nil {
			t.Fatal(err)
		}
		schema, err := plan.JSONSchema()
		if err != nil {
			t.Fatal(err)
		}
		fields := schema["properties"].(map[string]any)
		channels := fields["channels"].(map[string]any)
		if !reflect.DeepEqual(channels["items"].(map[string]any)["enum"], []any{"DISPLAY", "VIDEO"}) {
			t.Fatal(channels)
		}
		if _, exists := channels["enum"]; exists {
			t.Fatal("item choices became whole-array choices")
		}

		if !reflect.DeepEqual(fields["mode"].(map[string]any)["enum"], []any{int64(9007199254740993), int64(2), nil}) {
			t.Fatal(fields)
		}
		if !reflect.DeepEqual(fields["label"].(map[string]any)["examples"], []any{"a"}) {
			t.Fatal(fields)
		}
		if !reflect.DeepEqual(fields["label"].(map[string]any)["enum"], []any{"a", "b"}) {
			t.Fatal(fields)
		}
	}
}
