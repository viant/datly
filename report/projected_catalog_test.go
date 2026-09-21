package report

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestProjectedComponentsExposeCubeAndComposeSeparately(t *testing.T) {
	groupable := true
	cubeMCP, composeMCP := true, false
	component := &spec.Component{Key: spec.Key{Name: "Vendor"}, Description: "Vendor reader", RootView: &spec.View{Groupable: &groupable}, Routes: []*spec.Route{{Method: "GET", Path: "/vendors"}}, Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true, MCPTool: &cubeMCP, Compose: &spec.CubeComposeSettings{Enabled: true, MCPTool: &composeMCP}}}}
	actual := ProjectedComponents(component)
	if len(actual) != 2 || actual[0].Name != "VendorCube" || actual[0].Path != "/vendors/cube" || !actual[0].MCPEnabled || actual[1].Name != "VendorCubeCompose" || actual[1].Path != "/vendors/cube/compose" || actual[1].MCPEnabled {
		t.Fatalf("projected=%+v", actual)
	}
}
