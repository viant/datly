package report

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
	xcontract "github.com/viant/xdatly/contract"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

var reviewPolicyCalls atomic.Int64

type sourcePolicyRow groupedSpendRow
type sourcePolicyOutput struct{ Rows []*sourcePolicyRow }

func (*sourcePolicyRow) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Show    bool `parameter:",kind=header,in=X-Report-Show"`
		NoCache bool `parameter:",kind=header,in=X-Report-No-Cache"`
		Unnamed bool `parameter:",kind=header,in=X-Report-Unnamed"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("source-visibility:%t", state.Show)
	if state.Unnamed {
		name = ""
	}

	reviewPolicyCalls.Add(1)
	return &xcontract.Policy{Name: name, NoCache: state.NoCache, Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if !state.Show && parent == reflect.TypeFor[sourcePolicyRow]() && (field.Name == "TotalSpend" || field.Name == "Region") {
			return field.SetTag("mcp", "-"), nil
		}
		return nil, nil
	}}, nil
}
func TestSourceContractPropagatesToReportSelectorsAndCompose(t *testing.T) {
	h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}}).build(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	metadata, _, err := h.service.DescribeTool(t.Context(), entry.Metadata)
	require.NoError(t, err)
	in, _ := json.Marshal(metadata.InputSchema)
	out, _ := json.Marshal(metadata.OutputSchema)
	require.NotContains(t, string(in), "totalSpend", "protected measure must not be advertised")
	require.NotContains(t, string(out), "totalSpend", "source row policy must redact ordinary cube output")
	var inputSchema map[string]any
	require.NoError(t, json.Unmarshal(in, &inputSchema))
	properties := inputSchema["properties"].(map[string]any)
	dimensionSchema, _ := json.Marshal(properties["dimensions"])
	require.NotContains(t, string(dimensionSchema), "region", "protected dimension must not be advertised")
	filterSchema, _ := json.Marshal(properties["filters"])
	require.Contains(t, string(filterSchema), "region", "trusted source filter must remain bound")
	require.NotContains(t, string(out), "region", "source dimension output hidden")
	args := groupedReportRequest{Dimensions: []string{"Tenant", "AccountID", "Region"}, Measures: []string{"TotalSpend", "OrderCount"}, Filters: groupedReportFilters{AccountIDs: "1,2", Tenant: "acme", Region: "EU", Channel: "web", Status: "active"}}.mcpArguments()
	result, protocolErr := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: args}})
	require.Nil(t, protocolErr)
	require.True(t, result.IsError != nil && *result.IsError)
	body, _ := json.Marshal(result)
	require.NotContains(t, string(body), "totalSpend")
	compose, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	require.True(t, ok)
	result, protocolErr = compose.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}}}, "sql": "SELECT t1.AccountID,t1.TotalSpend AS protectedValue FROM $CubeSQL1 AS t1 ORDER BY t1.AccountID LIMIT 8"}}})
	require.Nil(t, protocolErr)
	body, _ = json.Marshal(result)
	t.Logf("compose result: %s; policy calls=%d", body, reviewPolicyCalls.Load())
	require.NotContains(t, string(body), "150", "compose alias cannot expose source metric")
	require.True(t, result.IsError != nil && *result.IsError)
}

func TestSourceContractNamedUnnamedAndNoCacheRevocation(t *testing.T) {
	for _, mode := range []string{"named", "unnamed", "nocache"} {
		t.Run(mode, func(t *testing.T) {
			h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}}).build(t)
			entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
			require.True(t, ok)
			for _, show := range []bool{false, true, true, false} {
				headers := http.Header{"X-Report-Show": {fmt.Sprint(show)}}
				if mode == "unnamed" {
					headers.Set("X-Report-Unnamed", "true")
				}
				if mode == "nocache" {
					headers.Set("X-Report-No-Cache", "true")
				}
				ctx := invocation.WithHTTPHeaders(t.Context(), headers)
				selected, _, err := h.service.DescribeTool(ctx, entry.Metadata)
				require.NoError(t, err)
				raw, err := json.Marshal(selected.InputSchema)
				require.NoError(t, err)
				require.Equal(t, show, strings.Contains(string(raw), "totalSpend"))
			}
		})
	}
}

func TestSourceContractNativeTransportRejectsEveryFrameBeforeQuery(t *testing.T) {
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		t.Run(version, func(t *testing.T) {
			h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}}).build(t)
			native := mcpclient.New(t, h.service, version)
			listing, err := native.ListTools(t.Context(), nil)
			require.NoError(t, err)
			wire, err := json.Marshal(listing)
			require.NoError(t, err)
			require.NotContains(t, string(wire), "totalSpend")
			filters := map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}
			for _, tc := range []struct {
				name, sql string
				frames    int
			}{
				{"alias", "SELECT t1.TotalSpend AS harmless FROM $CubeSQL1 AS t1 LIMIT 5", 1},
				{"calculation", "SELECT t1.TotalSpend/NULLIF(t1.OrderCount,0) AS harmless FROM $CubeSQL1 AS t1 LIMIT 5", 1},
				{"case", "SELECT CASE WHEN t1.TotalSpend>0 THEN t1.OrderCount ELSE 0 END AS harmless FROM $CubeSQL1 AS t1 LIMIT 5", 1},
				{"where", "SELECT t1.AccountID FROM $CubeSQL1 AS t1 WHERE t1.TotalSpend>0 LIMIT 5", 1},
				{"order", "SELECT t1.AccountID FROM $CubeSQL1 AS t1 ORDER BY t1.TotalSpend LIMIT 5", 1},
				{"join", "SELECT t1.AccountID FROM $CubeSQL1 AS t1 JOIN $CubeSQL2 AS t2 ON t1.AccountID=t2.AccountID AND t1.Region=t2.Region LIMIT 5", 2},
				{"late-union", "SELECT t1.AccountID AS key,NULL AS harmless FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.AccountID AS key,t2.TotalSpend AS harmless FROM $CubeSQL2 AS t2 LIMIT 5", 2},
			} {
				t.Run(tc.name, func(t *testing.T) {
					cubes := make([]any, tc.frames)
					for i := range cubes {
						cubes[i] = map[string]any{"filters": filters}
					}
					result, e := native.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: map[string]any{"cubes": cubes, "sql": tc.sql}})
					require.NoError(t, e)
					require.True(t, result.IsError != nil && *result.IsError)
					require.Zero(t, h.codec.calls.Load(), "forbidden late frame must not prepare an earlier query")
					raw, e := json.Marshal(result)
					require.NoError(t, e)
					require.NotContains(t, string(raw), "protectedValue\":150")
				})
			}
			args := groupedReportRequest{Dimensions: []string{"Tenant", "AccountID"}, Measures: []string{"OrderCount"}, Filters: groupedReportFilters{AccountIDs: "1,2", Tenant: "acme", Region: "EU", Channel: "web", Status: "active"}}.mcpArguments()
			result, err := native.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "SpendCube", Arguments: args})
			require.NoError(t, err)
			require.False(t, result.IsError != nil && *result.IsError)
			require.EqualValues(t, 1, h.codec.calls.Load(), "trusted region filter remains bound on admitted cube")
		})
	}
}

type sourceInputPolicy groupedSpendInput

func (*sourceInputPolicy) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "source-input:private-status", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[sourceInputPolicy]() && field.Name == "Status" {
			return field.SetTag("mcp", "-"), nil
		}
		return nil, nil
	}}, nil
}
func TestSourceInputPolicyProjectsOnlyExactFilterBinding(t *testing.T) {
	h := (groupedReportHarnessConfig{input: reflect.TypeFor[sourceInputPolicy](), output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }}}).build(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	metadata, _, err := h.service.DescribeTool(t.Context(), entry.Metadata)
	require.NoError(t, err)
	raw, err := json.Marshal(metadata.InputSchema)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "status")
	require.Contains(t, string(raw), "region", "output-only protection must not erase trusted input")
	compose, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	require.True(t, ok)
	metadata, _, err = h.service.DescribeTool(t.Context(), compose.Metadata)
	require.NoError(t, err)
	raw, err = json.Marshal(metadata.InputSchema)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "status")
	require.Contains(t, string(raw), "region")

}

func TestSourceContractMixedSelectorsKeepLenientUnknownAdmission(t *testing.T) {
	strict := false
	h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
		s.MCP = &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "SpendCube", StrictArguments: &strict}
	}}}).build(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	filters := map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}
	for _, args := range []map[string]any{
		{"dimensions": map[string]any{"accountId": true}, "measures": map[string]any{"totalSpend": true}},
		{"dimensions": map[string]any{"region": true}, "measures": map[string]any{"orderCount": true}},
	} {
		args["filters"] = filters
		result, rpc := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: args}})
		require.Nil(t, rpc)
		require.True(t, result.IsError != nil && *result.IsError)
		require.Zero(t, h.codec.calls.Load())
	}
	result, rpc := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"dimensions": map[string]any{"accountId": true, "unrelatedUnknown": true}, "measures": map[string]any{"orderCount": true}, "unrelatedTopLevel": 42, "filters": filters}}})
	require.Nil(t, rpc)
	require.False(t, result.IsError != nil && *result.IsError)
	require.EqualValues(t, 1, h.codec.calls.Load())
}

type sourceLinkedMeasures struct {
	TotalSpend bool `json:"totalSpend,omitempty" format:"inputNames=cost" mcp:"aliases=legacyCost"`
	OrderCount bool `json:"orderCount,omitempty" mcp:"aliases=oldCount"`
}
type sourceLinkedDimensions struct {
	Tenant    bool `json:"tenant,omitempty"`
	AccountID bool `json:"accountID,omitempty"`
	Region    bool `json:"region,omitempty" format:"inputNames=geo" mcp:"aliases=legacyGeo"`
}
type sourceLinkedInput struct {
	Dimensions sourceLinkedDimensions `json:"dimensions,omitempty"`
	Measures   sourceLinkedMeasures   `json:"measures,omitempty"`
	Filters    groupedLinkedFilters   `json:"filters,omitempty"`
	OrderBy    []string               `json:"orderBy,omitempty"`
	Limit      *int                   `json:"limit,omitempty"`
	Offset     *int                   `json:"offset,omitempty"`
}

func TestSourceContractRejectsNativeSelectorAliasesInMixedInput(t *testing.T) {
	strict := false
	h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
		s.LinkedInputType = "SourceLinkedInput"
		s.MCP = &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "SpendCube", StrictArguments: &strict}
	}}, source: func(_ *spec.Component, types *typecatalog.Catalog) {
		require.NoError(t, types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[sourceLinkedInput](), PkgPath: "example.com/acme/reporting", Name: "SourceLinkedInput"}))
	}}).build(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	for _, args := range []map[string]any{
		{"dimensions": map[string]any{"accountId": true}, "measures": map[string]any{"cost": true}},
		{"dimensions": map[string]any{"geo": true}, "measures": map[string]any{"orderCount": true}},
		{"dimensions": map[string]any{"accountId": true}, "measures": map[string]any{"legacyCost": true}},
		{"dimensions": map[string]any{"legacyGeo": true}, "measures": map[string]any{"orderCount": true}},
	} {
		args["filters"] = map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}
		result, rpc := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: args}})
		require.Nil(t, rpc)
		require.True(t, result.IsError != nil && *result.IsError)
		require.Zero(t, h.codec.calls.Load())
	}
}

func (*sourceLinkedInput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Mode    string `parameter:",kind=header,in=X-Report-Public-Naming"`
		NoCache bool   `parameter:",kind=header,in=X-Report-No-Cache"`
		Unnamed bool   `parameter:",kind=header,in=X-Report-Unnamed"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	name := "linked-selected:" + state.Mode
	if state.Unnamed {
		name = ""
	}
	return &xcontract.Policy{Name: name, NoCache: state.NoCache, Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if state.Mode == "goRename" && parent == reflect.TypeFor[sourceLinkedMeasures]() && field.Name == "TotalSpend" {
			renamed := field.Clone()
			renamed.Name = "RenamedSpend"
			return renamed, nil
		}
		if parent == reflect.TypeFor[sourceLinkedMeasures]() && field.Name == "OrderCount" {
			switch state.Mode {
			case "rename":
				return field.SetTag("mcp", "name=safeCount,aliases=currentCount"), nil
			case "collision":
				return field.SetTag("mcp", "name=legacyCost"), nil
			case "sibling":
				return field.SetTag("mcp", "name=legacyGeo"), nil
			}
		}
		return nil, nil
	}}, nil
}
func sourceLinkedPolicyHarness(t *testing.T) *groupedReportHarness {
	t.Helper()
	strict := false
	return (groupedReportHarnessConfig{output: reflect.TypeFor[sourcePolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
		s.LinkedInputType = "SourceLinkedInput"
		s.MCP = &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "SpendCube", StrictArguments: &strict}
	}}, source: func(_ *spec.Component, types *typecatalog.Catalog) {
		require.NoError(t, types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[sourceLinkedInput](), PkgPath: "example.com/acme/reporting", Name: "SourceLinkedInput"}))
	}}).build(t)
}
func TestSourceContractRecognitionKeepsSelectedPublicNamesAndScopes(t *testing.T) {
	h := sourceLinkedPolicyHarness(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	filters := map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}
	for _, mode := range []string{"named", "unnamed", "nocache"} {
		t.Run(mode, func(t *testing.T) {
			headers := http.Header{"X-Report-Public-Naming": {"rename"}}
			if mode == "unnamed" {
				headers.Set("X-Report-Unnamed", "true")
			}
			if mode == "nocache" {
				headers.Set("X-Report-No-Cache", "true")
			}
			ctx := invocation.WithHTTPHeaders(t.Context(), headers)
			metadata, _, err := h.service.DescribeTool(ctx, entry.Metadata)
			require.NoError(t, err)
			raw, err := json.Marshal(metadata.InputSchema)
			require.NoError(t, err)
			require.Contains(t, string(raw), "safeCount")
			require.NotContains(t, string(raw), "legacyCost")
			require.NotContains(t, string(raw), "oldCount")
			call := func(measures map[string]any) (*schema.CallToolResult, bool) {
				result, rpc := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"dimensions": map[string]any{"accountId": true}, "measures": measures, "filters": filters}}})
				return result, rpc != nil
			}
			before := h.codec.calls.Load()
			result, rpc := call(map[string]any{"legacyCost": true, "safeCount": true})
			require.False(t, rpc)
			require.True(t, result.IsError != nil && *result.IsError)
			require.Equal(t, before, h.codec.calls.Load())
			result, rpc = call(map[string]any{"safeCount": true})
			require.False(t, rpc)
			require.False(t, result.IsError != nil && *result.IsError)
			raw, err = json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.Contains(t, string(raw), "orderCount")
			result, rpc = call(map[string]any{"oldCount": true})
			require.False(t, rpc)
			require.False(t, result.IsError != nil && *result.IsError)
			raw, err = json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "orderCount", "removed public alias must stay removed")
			before = h.codec.calls.Load()
			_, rpc = call(map[string]any{"totalSpend": true, "legacyCost": true})
			require.True(t, rpc, "native canonical/alias conflict must remain an error")
			require.Equal(t, before, h.codec.calls.Load())
		})
	}
	for _, mode := range []string{"collision", "sibling"} {
		t.Run(mode, func(t *testing.T) {
			ctx := invocation.WithHTTPHeaders(t.Context(), http.Header{"X-Report-Public-Naming": {mode}})
			before := h.codec.calls.Load()
			result, rpc := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"dimensions": map[string]any{"accountId": true}, "measures": map[string]any{map[string]string{"collision": "legacyCost", "sibling": "legacyGeo"}[mode]: true}, "filters": filters}}})
			if mode == "collision" {
				require.NotNil(t, rpc)
				require.Equal(t, before, h.codec.calls.Load())
			} else {
				require.Nil(t, rpc)
				require.False(t, result.IsError != nil && *result.IsError)
				require.Equal(t, before+1, h.codec.calls.Load())
			}
		})
	}
}

func TestSourceContractUsesCanonicalSlotAfterDerivedGoRename(t *testing.T) {
	h := sourceLinkedPolicyHarness(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	ctx := invocation.WithHTTPHeaders(t.Context(), http.Header{"X-Report-Public-Naming": {"goRename"}})
	metadata, _, err := h.service.DescribeTool(ctx, entry.Metadata)
	require.NoError(t, err)
	raw, err := json.Marshal(metadata.InputSchema)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "totalSpend")
	require.NotContains(t, string(raw), "RenamedSpend")
	result, rpc := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"dimensions": map[string]any{"accountId": true}, "measures": map[string]any{"legacyCost": true, "orderCount": true}, "filters": map[string]any{"accountIDs": "1,2", "tenant": "acme", "region": "EU", "channel": "web", "status": "active"}}}})
	require.Nil(t, rpc)
	require.True(t, result.IsError != nil && *result.IsError)
	require.Zero(t, h.codec.calls.Load())
}
