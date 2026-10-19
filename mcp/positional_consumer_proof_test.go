package mcp

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	druntime "github.com/viant/datly/runtime"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type positionalProofBody struct {
	Value *string                      `json:"value" mcp:"Value,aliases=old"`
	Count int                          `json:"count"`
	Has   *struct{ Value, Count bool } `json:"-" setMarker:"true"`
}
type positionalProofInput struct {
	Query string                      `parameter:"Query,kind=query,in=q" json:"query" mcp:"QueryAlias,aliases=query"`
	Body  *positionalProofBody        `parameter:"Body,kind=body,in=data,required=false" json:"body" mcp:"Payload,aliases=body"`
	Has   *struct{ Body, Query bool } `json:"-" setMarker:"true"`
}
type positionalProofOutput struct {
	Query        string  `json:"query"`
	Value        *string `json:"value"`
	Count        int     `json:"count"`
	BodyNil      bool    `json:"bodyNil"`
	BodyPresent  bool    `json:"bodyPresent"`
	ValuePresent bool    `json:"valuePresent"`
}

func TestPrivatePositionalNamesActualCompileInvoke(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/explicit", Name: "Alias"}, Routes: []*spec.Route{{Method: "POST", Path: "/alias", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "original.tool.identity"}}}}}
	artifact, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[positionalProofInput](), OutputType: reflect.TypeFor[positionalProofOutput]()})
	require.NoError(t, e)
	var calls atomic.Int32
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[positionalProofOutput](), Handler: customhandler.NewFunc(func(ctx context.Context, in *positionalProofInput) (*positionalProofOutput, error) {
		calls.Add(1)
		out := &positionalProofOutput{Query: in.Query, BodyNil: in.Body == nil, BodyPresent: in.Has != nil && in.Has.Body}
		if in.Body != nil {
			out.Value = in.Body.Value
			out.Count = in.Body.Count
			out.ValuePresent = in.Body.Has != nil && in.Body.Has.Value
		}
		return out, nil
	})}
	runtime, e := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, e)
	service, e := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime, LinkedArtifact: &exec.LinkedArtifact{Revision: "private-proof", ContentFingerprint: strings.Repeat("a", 64)}})
	require.NoError(t, e)
	client := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, e := client.ListTools(context.Background(), nil)
	require.NoError(t, e)
	require.Len(t, listed.Tools, 1)
	require.Equal(t, "original.tool.identity", listed.Tools[0].Name)
	props := listed.Tools[0].InputSchema.Properties
	require.Contains(t, props, "QueryAlias")
	require.Contains(t, props, "Payload")
	require.NotContains(t, props, "body")
	nested := props["Payload"]["properties"].(map[string]any)
	require.Contains(t, nested, "Value")
	require.Contains(t, nested, "count")
	require.NotContains(t, nested, "value")
	require.NotContains(t, nested, "Has")
	for _, tc := range []struct {
		name  string
		args  map[string]any
		ok    bool
		value string
		query string
	}{{"missing", map[string]any{}, true, "", ""}, {"null", map[string]any{"Payload": nil}, true, "", ""}, {"empty", map[string]any{"Payload": map[string]any{}}, true, "", ""}, {"explicit", map[string]any{"QueryAlias": "q", "Payload": map[string]any{"Value": "hello", "count": 3}}, true, "hello", "q"}, {"empty string", map[string]any{"Payload": map[string]any{"Value": ""}}, true, "", ""}, {"nullable value", map[string]any{"Payload": map[string]any{"Value": nil}}, true, "", ""}, {"authored aliases", map[string]any{"query": "q", "body": map[string]any{"old": "legacy"}}, true, "legacy", "q"}, {"original nested JSON", map[string]any{"Payload": map[string]any{"value": "json"}}, true, "json", ""}, {"top-level conflict", map[string]any{"Payload": map[string]any{}, "body": map[string]any{}}, false, "", ""}, {"nested conflict", map[string]any{"Payload": map[string]any{"Value": "x", "value": "x"}}, false, "", ""}, {"nested alias conflict", map[string]any{"Payload": map[string]any{"Value": "x", "old": "x"}}, false, "", ""}, {"malformed", map[string]any{"Payload": map[string]any{"Value": map[string]any{"bad": true}}}, false, "", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			result, err := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "original.tool.identity", Arguments: tc.args})
			failed := err != nil || result == nil || (result.IsError != nil && *result.IsError)
			if !tc.ok {
				require.True(t, failed)
				require.Equal(t, before, calls.Load())
				return
			}
			require.NoError(t, err)
			require.False(t, failed)
			require.Equal(t, before+1, calls.Load())
			b, e := json.Marshal(result.StructuredContent)
			require.NoError(t, e)
			var out positionalProofOutput
			require.NoError(t, json.Unmarshal(b, &out))
			require.Equal(t, tc.query, out.Query)
			if tc.value != "" {
				require.NotNil(t, out.Value)
				require.Equal(t, tc.value, *out.Value)
			}
			if tc.name == "empty string" {
				require.NotNil(t, out.Value)
				require.Empty(t, *out.Value)
				require.True(t, out.ValuePresent)
			}
			if tc.name == "empty" {
				require.False(t, out.BodyNil)
				require.True(t, out.BodyPresent)
				require.False(t, out.ValuePresent)
			}
			if tc.name == "null" {
				require.True(t, out.BodyNil)
				require.True(t, out.BodyPresent)
			}
			if tc.name == "nullable value" {
				require.Nil(t, out.Value)
				require.True(t, out.ValuePresent)
			}
			if tc.name == "missing" {
				require.True(t, out.BodyNil)
				require.False(t, out.BodyPresent)
			}
		})
	}
}

type positionalHiddenCustom struct {
	Secret string `json:"secret" internal:"true" mcp:"SecretAlias"`
	Value  string `json:"value"`
}

func (p *positionalHiddenCustom) UnmarshalJSON(b []byte) error {
	type plain positionalHiddenCustom
	return json.Unmarshal(b, (*plain)(p))
}

type positionalHiddenInput struct {
	Body *positionalHiddenCustom `parameter:"Body,kind=body,in=data" json:"body"`
}
type positionalHiddenOutput struct {
	Value string `json:"value"`
}

func TestPrivateHiddenCustomBodyCompileInvoke(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/explicit", Name: "Hidden"}, Routes: []*spec.Route{{Method: "POST", Path: "/hidden", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "original.hidden"}}}}}
	artifact, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[positionalHiddenInput](), OutputType: reflect.TypeFor[positionalHiddenOutput]()})
	require.NoError(t, e)
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[positionalHiddenOutput](), Handler: customhandler.NewFunc(func(ctx context.Context, in *positionalHiddenInput) (*positionalHiddenOutput, error) {
		return &positionalHiddenOutput{Value: in.Body.Value}, nil
	})}
	runtime, e := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, e)
	service, e := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime, LinkedArtifact: &exec.LinkedArtifact{Revision: "private-proof", ContentFingerprint: strings.Repeat("a", 64)}})
	require.NoError(t, e)
	client := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, e := client.ListTools(context.Background(), nil)
	require.NoError(t, e)
	require.NotContains(t, listed.Tools[0].InputSchema.Properties["body"]["properties"], "SecretAlias")
	result, e := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "original.hidden", Arguments: map[string]any{"body": map[string]any{"value": "x"}}})
	require.NoError(t, e)
	require.False(t, result.IsError != nil && *result.IsError)
	b, e := json.Marshal(result.StructuredContent)
	require.NoError(t, e)
	require.JSONEq(t, `{"value":"x"}`, string(b))
}
