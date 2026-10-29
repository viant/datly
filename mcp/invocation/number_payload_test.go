package invocation

import (
	"context"
	"encoding/json"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
	"reflect"
	"testing"
)

func TestToolResultUsesLegacyJSONNumericProjection(t *testing.T) {
	const payload = `{"small":7,"large":9007199254740993,"maxUint64":18446744073709551615,"fraction":0.12345678901234567890123456789,"exponent":1e3,"tiny":1e-400,"nested":[9007199254740993,null],"text":"7","bool":true}`
	var want map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &want); err != nil {
		t.Fatal(err)
	}
	for name, execution := range map[string]*Execution{"success": {value: json.RawMessage(payload)}, "encoder": {value: true, encodeOutput: func(context.Context, any) ([]byte, error) { return []byte(payload), nil }}, "public error": {err: &response.Error{Code: 400, Payload: json.RawMessage(payload)}}} {
		t.Run(name, func(t *testing.T) {
			result := execution.ToolResult()
			got := result.StructuredContent.(map[string]interface{})
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("legacy projection changed: %#v", got)
			}
			if got["large"].(float64) != 9007199254740992 || got["small"].(float64) != 7 {
				t.Fatal("legacy extraction changed")
			}
			if result.Content[0].(schema.TextContent).Text != payload {
				t.Fatal("original text changed")
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Structured map[string]interface{} `json:"structuredContent"`
			}
			if err = json.Unmarshal(encoded, &wire); err != nil || !reflect.DeepEqual(want, wire.Structured) {
				t.Fatalf("wire differs: %s %v", encoded, err)
			}
		})
	}
}

func TestObjectPayloadRequiresSingleFiniteObject(t *testing.T) {
	for _, payload := range []string{"", "null", "[]", "7", `"text"`, `{"n":1e400}`, `{"n":1} {}`, `{"n":1} null`, `{"n":1} junk`, `{"n":`, `{"n":01}`, `{"n":NaN}`} {
		result := textToolResult([]byte(payload), nil)
		if result.StructuredContent != nil {
			t.Fatalf("invalid/non-object accepted: %s", payload)
		}
		if result.Content[0].(schema.TextContent).Text != payload {
			t.Fatal("original text changed")
		}
		if _, err := json.Marshal(result); err != nil {
			t.Fatal(err)
		}
	}
	if object := objectPayload([]byte(" \n {\"n\":7} \t")); object["n"].(float64) != 7 {
		t.Fatal("whitespace rejected")
	}
	if object := objectPayload([]byte(`{}`)); object == nil {
		t.Fatal("empty object rejected")
	}
}
