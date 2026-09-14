package tag

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestOutputSettingsTagRoundTrip(t *testing.T) {
	settings := SettingsFromSpec(&spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Rows.Secret", "Status.Trace"}, OmitEmpty: true, Title: "Report \"quoted\""}})
	tag, err := settings.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseSettings(reflect.StructTag(tag))
	if err != nil {
		t.Fatal(err)
	}
	var actual spec.Settings
	decoded.Apply(&actual)
	if !reflect.DeepEqual(actual.Output, settings.Output) {
		t.Fatalf("output %+v expected %+v", actual.Output, settings.Output)
	}
	actual.Output.Exclude[0] = "changed"
	if settings.Output.Exclude[0] != "Rows.Secret" {
		t.Fatal("metadata aliases source")
	}
}
