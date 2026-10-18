package mcp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	allselectors "github.com/viant/datly/bootstrap/testdata/selectorside"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/mcp-protocol/schema"
)

func TestSelectorSideInputsMCP(t *testing.T) {
	inputType, outputType := reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]()
	_, publicPage := inputType.FieldByName("Page")
	require.False(t, publicPage)
	service := selectorSideService(t, reflect.TypeFor[fixture.Component](), inputType, outputType, "ReadRecords")
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, err := native.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 1)
	encoded, err := json.Marshal(listed.Tools[0].InputSchema)
	require.NoError(t, err)
	var wire struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(encoded, &wire))
	require.Equal(t, "integer", wire.Properties["Page"].Type)
	require.Equal(t, "array", wire.Properties["Fields"].Type)
	for _, tc := range []struct {
		arguments map[string]any
		id        int
		label     string
	}{
		{map[string]any{"Fields": []string{"label"}, "Page": 1}, 0, "one"},
		{map[string]any{}, 1, "one"},
		{map[string]any{"Fields": []string{"id"}, "Page": 1}, 1, ""},
	} {
		result, err := native.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "records.select", Arguments: tc.arguments})
		require.NoError(t, err)
		require.NotNil(t, result)
		if result.IsError != nil {
			require.False(t, *result.IsError, "%+v", result)
		}
		body, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		var output fixture.Output
		require.NoError(t, json.Unmarshal(body, &output))
		require.Len(t, output.Data, 2)
		require.Equal(t, tc.id, output.Data[0].ID)
		require.Equal(t, tc.label, output.Data[0].Label)
	}
}

func selectorSideService(t *testing.T, holder, inputType, outputType reflect.Type, componentName string) *Service {
	t.Helper()
	discovered, err := bootstrap.ReflectSelectedPackages([]string{holder.PkgPath()}, nil)
	require.NoError(t, err)
	var component *spec.Component
	for _, source := range discovered.Components {
		if source.Tag.Name == componentName {
			component, err = source.Resolve(inputType, outputType)
			require.NoError(t, err)
			break
		}
	}
	require.NotNil(t, component)
	component.Routes[0].MCP = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.select"}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: outputType})
	require.NoError(t, err)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"))
	execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: inputType, OutputType: outputType, Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: outputType, Reader: execution}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	service, err := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: rt})
	require.NoError(t, err)
	return service
}

func TestSelectorSideOrderByMCP(t *testing.T) {
	service := selectorSideService(t, reflect.TypeFor[allselectors.Component](), reflect.TypeFor[allselectors.Input](), reflect.TypeFor[allselectors.Output](), "ReadRows")
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, err := native.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 1)
	encoded, err := json.Marshal(listed.Tools[0].InputSchema)
	require.NoError(t, err)
	var wire struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(encoded, &wire))
	require.Equal(t, "array", wire.Properties["OrderBy"].Type, "%s", encoded)
	require.NotContains(t, wire.Properties, "Sort")
	for _, tc := range []struct {
		args map[string]any
		ids  []int
	}{
		{map[string]any{"OrderBy": []string{"id DESC"}}, []int{2, 1}},
		{map[string]any{"OrderBy": []string{"id DESC"}, "Limit": 1, "Page": 2}, []int{1}},
		{map[string]any{}, []int{1, 2}},
	} {
		result, err := native.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "records.select", Arguments: tc.args})
		require.NoError(t, err)
		require.NotNil(t, result)
		if result.IsError != nil {
			require.False(t, *result.IsError, "%+v", result)
		}
		data, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		var output allselectors.Output
		require.NoError(t, json.Unmarshal(data, &output))
		var ids []int
		for _, row := range output.Data {
			ids = append(ids, row.ID)
		}
		require.Equal(t, tc.ids, ids)
	}
}
