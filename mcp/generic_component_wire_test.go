package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
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

func TestOrdinaryLookupWithArtifactUsesSchemaAndAuthWithoutComponentWireMeta(t *testing.T) {
	type input struct {
		Tenant int `parameter:"Tenant,kind=query,in=tenant" json:"tenant"`
	}
	type row struct {
		ID int `sqlx:"id" json:"id"`
	}
	type output struct {
		Rows []row `json:"rows"`
	}
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE ordinary_lookup (id INTEGER, tenant INTEGER)", "INSERT INTO ordinary_lookup VALUES (1,7),(2,8)"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/generic", Name: "Lookup"}, Routes: []*spec.Route{{Method: "GET", Path: "/lookup", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "lookup"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output]()})
	require.NoError(t, err)
	var calls atomic.Int32
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](), Handler: customhandler.NewFunc(func(ctx context.Context, in *input) (*output, error) {
		calls.Add(1)
		rows, err := db.ReadQuery(ctx, sqlite.Query{SQL: "SELECT id FROM ordinary_lookup WHERE tenant=?", Args: []any{in.Tenant}}, reflect.TypeFor[[]row]())
		if err != nil {
			return nil, err
		}
		return &output{Rows: rows.([]row)}, nil
	})}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	var denied atomic.Bool
	service, err := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime, LinkedArtifact: &exec.LinkedArtifact{Revision: "release-1", ContentFingerprint: strings.Repeat("a", 64)}, AuthorizeTool: func(context.Context, exec.ComponentTarget) error {
		if denied.Load() {
			return errors.New("synthetic policy denied")
		}
		return nil
	}})
	require.NoError(t, err)
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, err := native.ListTools(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 1)
	require.NotContains(t, listed.Tools[0].Meta, "viant.datly/component")
	result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "lookup", Arguments: map[string]any{"tenant": 7}})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, result.IsError != nil && *result.IsError)
	require.Equal(t, int32(1), calls.Load())
	raw, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	require.JSONEq(t, `{"rows":[{"id":1}]}`, string(raw))
	denied.Store(true)
	result, err = native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "lookup", Arguments: map[string]any{"tenant": 8}})
	require.True(t, err != nil || result == nil || result.IsError != nil && *result.IsError)
	require.Equal(t, int32(1), calls.Load())
	denied.Store(false)
	legacy := &schema.CallToolRequestParams{Name: "lookup", Arguments: map[string]any{"tenant": 7}}
	legacy.Meta.AdditionalProperties = map[string]any{"viant.datly/component": map[string]any{"revision": "release-1"}}
	_, err = native.CallTool(context.Background(), legacy)
	require.ErrorContains(t, err, "resource")
	require.Equal(t, int32(1), calls.Load())
}
