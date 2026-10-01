package dql

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestClientInputTypeGenerationOnly(t *testing.T) {
	component, err := parseComponentSource("example.com/read", "Records", `#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('ServerInput'))
#setting($_ = $client_input_type('RecordsRequest'))
SELECT id FROM records`)
	if err != nil {
		t.Fatal(err)
	}
	if component.Settings.Generation.ClientInputType != "RecordsRequest" || component.Settings.InputType != "ServerInput" {
		t.Fatalf("settings %+v", component.Settings)
	}
	if component.Settings.Clone().Generation.ClientInputType != "RecordsRequest" {
		t.Fatal("clone lost client shape")
	}
	raw, err := dtag.SettingsFromSpec(component.Settings).StructTag()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := dtag.ParseSettings(reflect.StructTag(raw))
	if err != nil {
		t.Fatal(err)
	}
	var runtime spec.Settings
	decoded.Apply(&runtime)
	if runtime.Generation != nil {
		t.Fatalf("runtime settings changed %+v", runtime)
	}
}
func TestClientInputTypeRejectsInvalidNames(t *testing.T) {
	for _, setting := range []string{"$client_input_type()", "$client_input_type('a','b')", "$client_input_type('private')", "$client_input_type('other.Request')", "$client_input_type('Request').Unknown()", "$client_input_type('Request')\n#setting($_ = $client_input_type('Second'))"} {
		t.Run(setting, func(t *testing.T) {
			if _, err := parseComponentSource("example.com/read", "Records", "#setting($_ = $route('/records','GET'))\n#setting($_ = "+setting+")\nSELECT id FROM records"); err == nil {
				t.Fatal("invalid client shape accepted")
			}
		})
	}
}
