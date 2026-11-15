package tag

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestReportMCPJSONTagRoundTrip(t *testing.T) {
	strict := false
	original := Component{Name: "Records", Path: "/records", Method: "GET", Report: true, ReportMCP: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "aggregate_records", Description: "Quoted, \"text\"\nsecond line", DescriptionPath: "docs/cube.md", CaseFormat: "lc", StrictArguments: &strict}, MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureResource, Name: "records", MIMEType: "application/json"}}}
	encoded, err := original.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	actual, ok, err := ParseComponent(reflect.StructTag(encoded))
	if err != nil || !ok {
		t.Fatalf("parse %v %v", ok, err)
	}
	if !reflect.DeepEqual(original, actual) {
		t.Fatalf("roundtrip %+v != %+v", original, actual)
	}
}
