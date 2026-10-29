package dql

import (
	"encoding/json"
	"github.com/viant/datly/spec"
	"testing"
)

func TestEnumDeclarationRoundTrip(t *testing.T) {
	for _, declaration := range []string{`$Mode<int64>(query/mode).Value(9007199254740993).WithEnum(9007199254740993,9007199254740994)`, `$Mode<float64>(query/mode).WithEnum(1.25,2.5)`, `$Mode<string>(query/mode).WithEnum("one","two")`, `$Mode<bool>(query/mode).WithEnum(false,true)`} {
		source := "#setting($_ = $route('/enum','GET'))\n#define($_ = " + declaration + ")\nSELECT 1 AS id"
		component, err := parseComponentSource("example.com/enum", "Enum", source)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(component)
		if err != nil {
			t.Fatal(err)
		}
		var loaded spec.Component
		if err = json.Unmarshal(raw, &loaded); err != nil {
			t.Fatal(err)
		}
		if len(loaded.Parameters[0].WireSchema.Enum) != 2 {
			t.Fatal("enum lost")
		}
		exported := (Serializer{}).Export(&loaded, source)
		reparsed, err := parseComponentSource("example.com/enum", "Enum", exported.Source)
		if err != nil || len(reparsed.Parameters[0].WireSchema.Enum) != 2 {
			t.Fatalf("enum round trip: %v", err)
		}
	}
	for _, bad := range []string{`.WithEnum()`, `.WithEnum(null)`, `.WithEnum({"a":1})`, `.WithEnum(1).WithEnum(2)`} {
		if _, err := parseComponentSource("example.com/enum", "Enum", "#setting($_ = $route('/enum','GET'))\n#define($_ = $Mode<int64>(query/mode)"+bad+")\nSELECT 1 AS id"); err == nil {
			t.Fatal(bad)
		}
	}
}
