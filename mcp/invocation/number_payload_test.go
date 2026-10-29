package invocation

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

func TestToolResultPreservesNumericJSONOnWire(t *testing.T) {
	const payload = `{"small":7,"large":9007199254740993,"maxUint64":18446744073709551615,"negative":-9007199254740993,"fraction":0.12345678901234567890123456789,"exponent":1.234567890123456789e+100,"zero":-0,"nil":null,"nested":{"items":[9007199254740993,null,[18446744073709551615,1e-400]]},"text":"9007199254740993","bool":true}`
	for name, execution := range map[string]*Execution{
		"success":        {value: json.RawMessage(payload)},
		"output encoder": {value: true, encodeOutput: func(context.Context, any) ([]byte, error) { return []byte(payload), nil }},
		"public error":   {err: &response.Error{Code: 400, Payload: json.RawMessage(payload)}},
	} {
		t.Run(name, func(t *testing.T) {
			result := execution.ToolResult()
			wire, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			// Inspect actual protocol bytes without using the protocol's own unmarshaller.
			var envelope struct {
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
				Structured json.RawMessage `json:"structuredContent"`
			}
			if err := json.Unmarshal(wire, &envelope); err != nil {
				t.Fatal(err)
			}
			if len(envelope.Content) != 1 || envelope.Content[0].Type != "text" || envelope.Content[0].Text != payload {
				t.Fatalf("text payload changed: %s", wire)
			}
			if !reflect.DeepEqual(decodeNumberOracle(t, []byte(payload)), decodeNumberOracle(t, envelope.Structured)) {
				t.Fatalf("numeric tokens changed: %s", wire)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(envelope.Structured, &fields); err != nil {
				t.Fatal(err)
			}
			if string(fields["large"]) != "9007199254740993" || string(fields["maxUint64"]) != "18446744073709551615" {
				t.Fatalf("integers changed or became strings: %s", wire)
			}
			object := result.StructuredContent.(map[string]interface{})
			if object["small"] != json.Number("7") {
				t.Fatalf("Go numeric value = %T (%v)", object["small"], object["small"])
			}
		})
	}
}

func TestToolResultPreservesExtremeExponentOnWire(t *testing.T) {
	const payload = `{"large":1e400,"small":1e-400}`
	wire, err := json.Marshal((&Execution{value: json.RawMessage(payload)}).ToolResult())
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Structured json.RawMessage `json:"structuredContent"`
	}
	if err := json.Unmarshal(wire, &envelope); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodeNumberOracle(t, []byte(payload)), decodeNumberOracle(t, envelope.Structured)) {
		t.Fatalf("exponent tokens changed: %s", wire)
	}
}

func decodeNumberOracle(t *testing.T, raw []byte) interface{} {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return value
}

func TestObjectPayloadRequiresSingleCompleteObject(t *testing.T) {
	for _, payload := range []string{"", "null", "[]", "7", `"text"`, `{"n":1} {"n":2}`, `{"n":1} null`, `{"n":1} junk`, `{"n":`, `{"n":01}`, `{"n":NaN}`} {
		t.Run(payload, func(t *testing.T) {
			result := textToolResult([]byte(payload), nil)
			if result.StructuredContent != nil {
				t.Fatalf("invalid/non-object payload accepted: %#v", result.StructuredContent)
			}
			wire, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(wire, &fields); err != nil {
				t.Fatal(err)
			}
			if _, present := fields["structuredContent"]; present {
				t.Fatalf("structured content present: %s", wire)
			}
			if result.Content[0].(schema.TextContent).Text != payload {
				t.Fatal("text changed")
			}
		})
	}
	if result := objectPayload([]byte(" \n {\"n\":7} \t\n")); result["n"] != json.Number("7") {
		t.Fatalf("whitespace rejected: %#v", result)
	}
	if result := objectPayload([]byte(`{}`)); result == nil {
		t.Fatal("empty object rejected")
	}
}
