package report

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

func (h *groupedReportHarness) invoke(t *testing.T, request groupedReportRequest) *groupedSpendOutput {
	t.Helper()
	input := h.input(t, request)
	component := h.artifact.Component()
	actual, err := h.runtime.InvokeComponent(context.Background(), exec.ComponentRequest{
		Target: exec.ComponentTarget{
			Component: component.Key,
			Route:     spec.RouteRef{Method: component.Routes[0].Method, Path: component.Routes[0].Path},
		},
		Input: input,
	})
	if err != nil {
		t.Fatalf("invoke grouped report: %v", err)
	}
	output, ok := actual.(*groupedSpendOutput)
	if !ok {
		t.Fatalf("grouped report output type = %T", actual)
	}
	return output
}

func (h *groupedReportHarness) invokeMCP(t *testing.T, request groupedReportRequest) *groupedSpendOutput {
	t.Helper()
	tool, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	if !ok {
		t.Fatal("grouped report MCP tool is unavailable")
	}
	result, protocolErr := tool.Handler(context.Background(), &schema.CallToolRequest{
		Method: schema.MethodToolsCall,
		Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: request.mcpArguments()},
	})
	if protocolErr != nil || result == nil || result.IsError != nil && *result.IsError {
		t.Fatalf("invoke grouped report through MCP: result=%+v error=%+v", result, protocolErr)
	}
	content, ok := result.Content[0].(schema.TextContent)
	if !ok {
		t.Fatalf("grouped report MCP content = %T", result.Content[0])
	}
	actual := &groupedSpendOutput{}
	if err := json.Unmarshal([]byte(content.Text), actual); err != nil {
		t.Fatalf("decode grouped MCP result: %v", err)
	}
	return actual
}

func (h *groupedReportHarness) input(t *testing.T, request groupedReportRequest) interface{} {
	t.Helper()
	input := reflect.New(h.artifact.InputType())
	setSelections(t, input.Elem().FieldByName("Dimensions"), request.Dimensions)
	setSelections(t, input.Elem().FieldByName("Measures"), request.Measures)
	filters := input.Elem().FieldByName("Filters")
	for _, item := range request.filterValues() {
		if item.value == "" {
			continue
		}
		field := filters.FieldByName(item.name)
		if !field.IsValid() || field.Type() != reflect.TypeOf((*string)(nil)) {
			t.Fatalf("report filter %s type = %v", item.name, field.Type())
		}
		value := item.value
		field.Set(reflect.ValueOf(&value))
	}
	input.Elem().FieldByName("OrderBy").Set(reflect.ValueOf(append([]string(nil), request.OrderBy...)))
	if request.Limit != nil {
		value := *request.Limit
		input.Elem().FieldByName("Limit").Set(reflect.ValueOf(&value))
	}
	if request.Offset != nil {
		value := *request.Offset
		input.Elem().FieldByName("Offset").Set(reflect.ValueOf(&value))
	}
	return input.Interface()
}

func setSelections(t *testing.T, section reflect.Value, names []string) {
	t.Helper()
	for _, name := range names {
		field := section.FieldByName(name)
		if !field.IsValid() || field.Kind() != reflect.Bool {
			t.Fatalf("report selection %s is unavailable", name)
		}
		field.SetBool(true)
	}
}

func (r groupedReportRequest) filterValues() []groupedFilterValue {
	return []groupedFilterValue{
		{name: "AccountIDs", value: r.Filters.AccountIDs},
		{name: "Tenant", value: r.Filters.Tenant},
		{name: "Region", value: r.Filters.Region},
		{name: "Channel", value: r.Filters.Channel},
		{name: "Status", value: r.Filters.Status},
	}
}

func (r groupedReportRequest) mcpArguments() map[string]interface{} {
	dimensions := map[string]interface{}{}
	for _, name := range r.Dimensions {
		dimensions[lowerCamel(name)] = true
	}
	measures := map[string]interface{}{}
	for _, name := range r.Measures {
		measures[lowerCamel(name)] = true
	}
	filters := map[string]interface{}{}
	for _, item := range r.filterValues() {
		if item.value != "" {
			filters[lowerCamel(item.name)] = item.value
		}
	}
	result := map[string]interface{}{"dimensions": dimensions, "measures": measures, "filters": filters}
	if len(r.OrderBy) > 0 {
		result["orderBy"] = append([]string(nil), r.OrderBy...)
	}
	if r.Limit != nil {
		result["limit"] = *r.Limit
	}
	if r.Offset != nil {
		result["offset"] = *r.Offset
	}
	return result
}
