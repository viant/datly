package openapi3

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"strings"
	"testing"
)

func TestSchemaYAMLRetainsExactIntegerBounds(t *testing.T) {
	minimum, maximum := json.Number("-9223372036854775808"), json.Number("9223372036854775807")
	source := Schema{Type: "integer", Min: &minimum, Max: &maximum}
	data, err := yaml.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "-9223372036854775808") || !strings.Contains(string(data), "9223372036854775807") {
		t.Fatalf("rounded YAML: %s", data)
	}
	var restored Schema
	if err = yaml.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Min == nil || restored.Max == nil || *restored.Min != minimum || *restored.Max != maximum {
		t.Fatalf("roundtrip: %+v", restored)
	}
}
