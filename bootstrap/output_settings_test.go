package bootstrap

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestRouteSourceResolveRetainsOutputOnlySettings(t *testing.T) {
	for _, output := range []*spec.OutputSettings{
		{Exclude: []string{"Rows.Secret"}},
		{OmitEmpty: true},
		{Title: "Records"},
		{},
	} {
		source := &RouteSource{FieldName: "Records", Tag: dtag.Component{
			Method: "GET", Path: "/records", Settings: dtag.Settings{Output: output},
		}}
		component, err := source.Resolve(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if component.Settings == nil || !reflect.DeepEqual(component.Settings.Output, output) {
			t.Fatalf("output-only settings lost: got %+v, want %+v", component.Settings, output)
		}
		if len(output.Exclude) != 0 {
			component.Settings.Output.Exclude[0] = "changed"
			if output.Exclude[0] != "Rows.Secret" {
				t.Fatal("resolved settings alias authored metadata")
			}
		}
	}
}
