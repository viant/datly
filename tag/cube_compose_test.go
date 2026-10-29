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

func TestComposeValuesTypeTagRoundTrip(t *testing.T) {
	original := Component{Name: "Metrics", ReportCompose: &spec.CubeComposeSettings{Enabled: true, ValuesType: "controls.Scalars"}}
	raw, err := original.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	parsed, ok, err := ParseComponent(reflect.StructTag(raw))
	if err != nil || !ok || parsed.ReportCompose == nil || parsed.ReportCompose.ValuesType != "controls.Scalars" {
		t.Fatalf("scalar type lost: %+v %v", parsed, err)
	}
}

func TestComposeInlineValuesTypeTagRoundTrip(t *testing.T) {
	shape := "struct { Metric int `json:\"metric,omitempty\" choice:\"1\" choice:\"2\" validate:\"choice(1,2)\"` }"
	original := Component{Name: "Metrics", ReportCompose: &spec.CubeComposeSettings{Enabled: true, ValuesType: shape}}
	raw, err := original.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	parsed, ok, err := ParseComponent(reflect.StructTag(raw))
	if err != nil || !ok || parsed.ReportCompose == nil || parsed.ReportCompose.ValuesType != shape {
		t.Fatalf("inline shape lost: %s %+v %v", raw, parsed, err)
	}
}
