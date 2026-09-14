package dql

import (
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestOutputSettingsAuthoredPackageRoundTrip(t *testing.T) {
	component, err := parseComponentSource("example.com/output", "Records", `#setting($_ = $route('/records', 'GET'))
#setting($_ = $format('csv'))
#setting($_ = $output_exclude('Rows.Secret', 'Status.Trace'))
#setting($_ = $output_omit_empty(true))
#setting($_ = $output_title('Records export'))
SELECT id, name FROM records`)
	if err != nil {
		t.Fatal(err)
	}
	want := &spec.OutputSettings{Exclude: []string{"Rows.Secret", "Status.Trace"}, OmitEmpty: true, Title: "Records export"}
	if component.Settings == nil || !reflect.DeepEqual(component.Settings.Output, want) {
		t.Fatalf("authored output %+v", component.Settings)
	}
	tag, err := dtag.SettingsFromSpec(component.Settings).StructTag()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := dtag.ParseSettings(reflect.StructTag(tag))
	if err != nil {
		t.Fatal(err)
	}
	var actual spec.Settings
	decoded.Apply(&actual)
	if actual.Format != "csv" || !reflect.DeepEqual(actual.Output, want) {
		t.Fatalf("package output %+v", actual)
	}
}
