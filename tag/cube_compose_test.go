package tag

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestCubeComposeTagRoundTrip(t *testing.T) {
	disabled := false
	component := Component{Name: "Metrics", Path: "/metrics", Method: "GET", Report: true, ReportCompose: &spec.CubeComposeSettings{Enabled: true, MCPTool: &disabled, MaxCubes: 12, MaxLimit: 50, TimeoutMs: 5000}}
	tag, err := component.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	parsed, ok, err := ParseComponent(reflect.StructTag(tag))
	if err != nil || !ok {
		t.Fatalf("parse: %v", err)
	}
	if !reflect.DeepEqual(component.ReportCompose, parsed.ReportCompose) {
		t.Fatalf("compose=%+v", parsed.ReportCompose)
	}
}
