package openapi3

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestAdditionalPropertiesAuthorityRoundTrip(t *testing.T) {
	for _, text := range []string{`{"type":"object","additionalProperties":false}`, `{"type":"object","additionalProperties":true}`, `{"type":"object","additionalProperties":{"type":"string"}}`} {
		var schema Schema
		if err := json.Unmarshal([]byte(text), &schema); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(schema)
		if err != nil {
			t.Fatal(err)
		}
		var before, after any
		_ = json.Unmarshal([]byte(text), &before)
		_ = json.Unmarshal(data, &after)
		a, _ := json.Marshal(before)
		b, _ := json.Marshal(after)
		if string(a) != string(b) {
			t.Fatalf("%s != %s", a, b)
		}
		encoded, err := yaml.Marshal(schema)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Schema
		if err = yaml.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
	}
}
func TestUnsupportedSchemaDoesNotSilentlyChangeAuthority(t *testing.T) {
	for _, text := range []string{`{"type":"object","additionalProperties":null}`, `{"type":"string","const":"fixed"}`} {
		var schema Schema
		if err := json.Unmarshal([]byte(text), &schema); err == nil {
			t.Fatal(text)
		}
	}
}
