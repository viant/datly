package dql

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestWriterOmitEmptyIsGenerationOnly(t *testing.T) {
	component, err := parseComponentSource("example.com/writer", "Records", `#setting($_ = $route('/records','PATCH'))
#setting($_ = $writer_omit_empty(true))
#setting($_ = $case_format('lc'))
SELECT id FROM records`)
	if err != nil {
		t.Fatal(err)
	}
	if component.Settings == nil || component.Settings.Generation == nil || !component.Settings.Generation.WriterOmitEmpty {
		t.Fatalf("missing generation policy %+v", component.Settings)
	}
	if component.Settings.Output != nil {
		t.Fatal("generation policy changed runtime output settings")
	}
	clone := component.Settings.Clone()
	if !clone.Generation.WriterOmitEmpty {
		t.Fatal("clone lost policy")
	}
	raw, err := dtag.SettingsFromSpec(component.Settings).StructTag()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := dtag.ParseSettings(reflect.StructTag(raw))
	if err != nil {
		t.Fatal(err)
	}
	var settings spec.Settings
	decoded.Apply(&settings)
	if settings.Generation != nil || settings.Output != nil {
		t.Fatal("generation policy escaped into runtime package settings")
	}
}

func TestWriterOmitEmptyRejectsInvalidArguments(t *testing.T) {
	for _, source := range []string{"$writer_omit_empty()", "$writer_omit_empty(true,false)", "$writer_omit_empty('sometimes')", "$writer_omit_empty(true).Unexpected()"} {
		t.Run(source, func(t *testing.T) {
			if _, err := parseComponentSource("example.com/writer", "Records", "#setting($_ = $route('/records','PATCH'))\n#setting($_ = "+source+")\nSELECT id FROM records"); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
}
