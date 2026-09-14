package tag

import (
	"reflect"
	"testing"

	"github.com/viant/assertly"
	"github.com/viant/datly/spec"
	xdocs "github.com/viant/xdatly/docs"
)

func TestComponentRoundTrip(t *testing.T) {
	original := Component{RouteName: "List", Path: "/v1/api/vendors", Method: "GET", View: "VendorView", Connector: "analytics", Handler: "VendorHandler", APIKeyHeader: "X-Key", APIKeyValue: "secret", Description: "Vendor lookup", Example: `{"id":1}`}
	structTag, err := original.StructTag()
	if err != nil {
		t.Fatalf("StructTag() error = %v", err)
	}
	assertly.AssertValues(t, `component:",path=/v1/api/vendors,method=GET,connector=analytics,handler=VendorHandler,view=VendorView" routeName:"List" apiKeyHeader:"X-Key" apiKeyValue:"secret" desc:"Vendor lookup" example:"{\"id\":1}"`, structTag)
	actual, ok, err := ParseComponent(reflect.StructTag(structTag))
	if err != nil || !ok {
		t.Fatalf("ParseComponent() = %+v, %v, %v", actual, ok, err)
	}
	assertly.AssertValues(t, original, actual)
}

func TestComponentRoundTripPreservesRouteMCPExposures(t *testing.T) {
	original := Component{
		Name: "Orders", Path: "/orders", Method: "GET",
		MCP: []*spec.MCPExposure{
			{Kind: spec.MCPExposureTool, Name: "orders.search", Description: "Search orders"},
			{Kind: spec.MCPExposureResource, Name: "orders", MIMEType: "application/json"},
		},
	}
	structTag, err := original.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	actual, ok, err := ParseComponent(reflect.StructTag(structTag))
	if err != nil || !ok {
		t.Fatalf("ParseComponent() = %+v, %v, %v", actual, ok, err)
	}
	assertly.AssertValues(t, original, actual)
}

func TestParseComponentRejectsMalformedRouteMCPTag(t *testing.T) {
	_, _, err := ParseComponent(reflect.StructTag(`component:",path=/orders,method=GET" mcp:"not-json"`))
	if err == nil {
		t.Fatal("expected malformed route MCP tag error")
	}
}

func TestParseComponentRejectsRemovedSummaryOption(t *testing.T) {
	if _, err := ParseComponentValue(",path=/v1/vendors,method=GET,summary=Summary"); err == nil {
		t.Fatal("expected removed summary option to fail")
	}
}

func TestComponentRoundTripPreservesAPIKeyDelimiters(t *testing.T) {
	original := Component{Path: `/v1/vendors?active=true`, Method: "GET", APIKeyValue: " secret,`quoted`=\"value\" "}
	structTag, err := original.StructTag()
	if err != nil {
		t.Fatalf("StructTag() error = %v", err)
	}
	actual, ok, err := ParseComponent(reflect.StructTag(structTag))
	if err != nil || !ok {
		t.Fatalf("ParseComponent() = %+v, %v, %v", actual, ok, err)
	}
	assertly.AssertValues(t, original, actual)
}

func TestComponentValidation(t *testing.T) {
	if err := (Component{Path: "/v1/vendors"}).ValidateRoute(); err == nil {
		t.Fatal("expected missing method error")
	}
	if _, err := (Component{Name: "vendor,bad", Path: "/v1/vendors", Method: "GET"}).StructTag(); err == nil {
		t.Fatal("expected invalid name delimiter error")
	}
	if _, _, err := ParseComponent(reflect.StructTag(`component:",path=/v1/vendors,broken"`)); err == nil {
		t.Fatal("expected malformed component tag error")
	}
}

func TestParseComponentReportShape(t *testing.T) {
	disabled := false
	actual, err := ParseComponentValue(",path=/v1/api/vendors,method=GET,report=true,reportMCPTool=false,reportLinkedInputType=In,reportDimensions=Dims,reportMeasures=Measures,reportFilters=Filters,reportOrderBy=Sort,reportLimit=Take,reportOffset=Skip")
	if err != nil {
		t.Fatalf("ParseComponentValue() error = %v", err)
	}
	expected := Component{
		Path: "/v1/api/vendors", Method: "GET", Report: true, ReportMCPTool: &disabled, ReportLinkedInputType: "In",
		ReportDimensions: "Dims", ReportMeasures: "Measures", ReportFilters: "Filters",
		ReportOrderBy: "Sort", ReportLimit: "Take", ReportOffset: "Skip",
	}
	assertly.AssertValues(t, expected, actual)
	structTag, err := actual.StructTag()
	if err != nil {
		t.Fatalf("StructTag() error = %v", err)
	}
	roundTrip, ok, err := ParseComponent(reflect.StructTag(structTag))
	if err != nil || !ok {
		t.Fatalf("ParseComponent() = %+v, %v, %v", roundTrip, ok, err)
	}
	assertly.AssertValues(t, expected, roundTrip)
}

func TestParseComponentRejectsMalformedReport(t *testing.T) {
	if _, err := ParseComponentValue(",path=/v1/vendors,method=GET,report=yes"); err == nil {
		t.Fatal("expected malformed report boolean error")
	}
	if _, err := ParseComponentValue(",path=/v1/vendors,method=GET,reportInput=Input"); err == nil {
		t.Fatal("expected obsolete reportInput key error")
	}
}

func TestParseComponentAbsent(t *testing.T) {
	_, ok, err := ParseComponent(reflect.StructTag(`json:"name"`))
	if err != nil || ok {
		t.Fatalf("ParseComponent() absent = %v, %v", ok, err)
	}
}

func TestDocumentationSourceRoundTrip(t *testing.T) {
	original := Component{Path: "/docs", Method: "GET", Documentation: xdocs.Source{BaseURL: "pkg:", DocURL: "one.yaml", DocURLs: []string{"two.yaml", "three.yaml"}, Substitutes: map[string]string{"table": "orders"}}}
	encoded, err := original.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	actual, ok, err := ParseComponent(reflect.StructTag(encoded))
	if err != nil || !ok {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, actual) {
		t.Fatalf("round trip: %+v", actual)
	}
}
