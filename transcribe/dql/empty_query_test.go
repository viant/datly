package dql

import (
	"reflect"
	"testing"

	dtag "github.com/viant/datly/tag"
)

func TestEmptyQueryPolicyAuthoringRoundtrip(t *testing.T) {
	for _, value := range []string{"true", "false", "invalid"} {
		component, err := parseComponentSource("example.com/records", "Records", "#setting($_ = $route('/records','GET'))\n#setting($_ = $ignoreEmptyQueryParameters("+value+"))\nSELECT 1")
		if value == "invalid" {
			if err == nil {
				t.Fatal("invalid policy accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if component.Settings == nil || component.Settings.IgnoreEmptyQueryParameters == nil || *component.Settings.IgnoreEmptyQueryParameters != (value == "true") {
			t.Fatal("DQL policy lost")
		}
		encoded, err := dtag.SettingsFromSpec(component.Settings).StructTag()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := dtag.ParseSettings(reflect.StructTag(encoded))
		if err != nil {
			t.Fatal(err)
		}
		if decoded.IgnoreEmptyQueryParameters == nil || *decoded.IgnoreEmptyQueryParameters != (value == "true") {
			t.Fatal("Go tag policy lost")
		}
	}
}
