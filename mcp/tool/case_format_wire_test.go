package tool

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestCaseFormatWithNestedWireSchema(t *testing.T) {
	type body struct {
		AccountID int
		EventDate string
	}
	type input struct{ Body body }
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body", In: "body"}, Extension: &spec.Parameter{WireSchemas: map[string]*spec.WireSchema{"Body.EventDate": {Type: "string", Format: "date", Required: true}}}}})
	plan, err := NewCompiler().Compile(Input{CaseFormat: "lu", Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "record"}})
	require.NoError(t, err)
	object := plan.Metadata().InputSchema.Properties["body"]
	properties := object["properties"].(map[string]any)
	require.Equal(t, "date", properties["event_date"].(map[string]any)["format"])
	require.Contains(t, object["required"], "event_date")
	scope, err := plan.Scope(map[string]any{"body": map[string]any{"account_id": float64(17), "event_date": "2026-10-27"}})
	require.NoError(t, err)
	defer scope.Close()
	value, ok, err := scope.Body().Locate(nil).Value(context.Background(), reflect.TypeFor[body](), "body")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, body{AccountID: 17, EventDate: "2026-10-27"}, value)
}

type caseFormatCustomBody struct{ Child struct{ EventDate string } }

func (*caseFormatCustomBody) UnmarshalJSON([]byte) error {
	panic("schema must not call custom decoder")
}
func TestCaseFormatCustomOwnerKeepsWireOverlay(t *testing.T) {
	projector := schemaProjector{caseFormat: "lu", schemas: map[string]*spec.WireSchema{"Body.Child.EventDate": {Type: "string", Format: "date"}}}
	object, err := projector.argument(reflect.TypeFor[caseFormatCustomBody](), "Body", "body", nil)
	require.NoError(t, err)
	child := object["properties"].(map[string]any)["Child"].(map[string]any)
	event := child["properties"].(map[string]any)["EventDate"].(map[string]any)
	require.Equal(t, "date", event["format"])
}
func TestCaseFormatAnonymousCustomOwnerKeepsMemberNames(t *testing.T) {
	type input struct {
		Body caseFormatCustomBody `anonymous:"true"`
	}
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{CaseFormat: "lu", Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "record"}})
	require.NoError(t, err)
	properties := plan.Metadata().InputSchema.Properties
	require.Contains(t, properties, "Child")
	child := properties["Child"]["properties"].(map[string]any)
	require.Contains(t, child, "EventDate")
}

type caseFormatPlainBody struct{ Child struct{ EventDate string } }
type caseFormatCustomSlice []caseFormatPlainBody

func (*caseFormatCustomSlice) UnmarshalJSON([]byte) error { panic("must not execute decoder") }
func TestCaseFormatCustomContainerKeepsWireOverlay(t *testing.T) {
	projector := schemaProjector{caseFormat: "lu", schemas: map[string]*spec.WireSchema{"Body.Child.EventDate": {Type: "string", Format: "date"}}}
	object, err := projector.argument(reflect.TypeFor[caseFormatCustomSlice](), "Body", "body", nil)
	require.NoError(t, err)
	item := object["items"].(map[string]any)
	child := item["properties"].(map[string]any)["Child"].(map[string]any)
	event := child["properties"].(map[string]any)["EventDate"].(map[string]any)
	require.Equal(t, "date", event["format"])
}
