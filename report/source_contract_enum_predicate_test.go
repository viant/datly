package report

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/viant/bindly"
	httpgateway "github.com/viant/datly/gateway/http"
	xcontract "github.com/viant/xdatly/contract"
	xmcp "github.com/viant/xdatly/handler/mcp"
	xreader "github.com/viant/xdatly/reader"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
)

type contractEnumScalarInput struct {
	Code int `mcpEnum:"true" choice:"1:first" choice:"2:second" enumAccept:"literal"`
}
type contractEnumScalarUnionInput struct {
	Code int `mcpEnum:"true" choice:"1:first" choice:"2:second" enumAccept:"scalar" enumAlias:"1:1" enumAlias:"2:2"`
}
type contractEnumFallbackInput struct {
	Code int `mcpEnum:"true" choice:"1:first" choice:"2:second" enumAccept:"scalar" enumUnknownString:"2"`
}
type contractEnumLargeInput struct {
	Code int64 `mcpEnum:"true" choice:"9007199254740993:huge" enumAccept:"scalar" enumAlias:"9007199254740993:9007199254740993"`
}
type contractEnumCollectionInput struct {
	Codes    []int    `mcpEnum:"true" choice:"1:first" choice:"2:second" enumAccept:"literal"`
	Channels []string `mcpEnum:"true" choice:"web" choice:"store" enumAccept:"literal"`
}

type enumPredicateHarness struct {
	*groupedReportHarness
	queries *atomic.Int64
}

func enumPredicateReportHarness(t *testing.T, inputType reflect.Type, configure ...func(*spec.Component, *typecatalog.Catalog)) *enumPredicateHarness {
	calls := &atomic.Int64{}
	h := (groupedReportHarnessConfig{input: inputType, queryGuard: func(_ context.Context, _ *sql.DB, query *xreader.ParameterizedQuery) error {
		calls.Add(1)
		t.Logf("report predicate bound args=%#v", query.Args)
		for _, arg := range query.Args {
			if arg == "first" || arg == "second" {
				t.Error("literal enum token reached SQL instead of stored value")
			}
		}
		return nil
	}, source: func(c *spec.Component, types *typecatalog.Catalog) {
		c.Parameters = []*spec.Parameter{
			{Name: "Code", Source: spec.BindSource{Kind: "query", Name: "code"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"s", "account_id"}}}},
			{Name: "Codes", Source: spec.BindSource{Kind: "query", Name: "codes"}, Predicates: []*spec.Predicate{{Name: "in", Args: []string{"s", "account_id"}}}},
			{Name: "Channels", Source: spec.BindSource{Kind: "query", Name: "channels"}, Predicates: []*spec.Predicate{{Name: "in", Args: []string{"s", "channel"}}}},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		}
		parameters := c.Parameters
		c.Parameters = nil
		for _, parameter := range parameters {
			if field, ok := inputType.FieldByName(parameter.Name); ok {
				parameter.Tag = string(field.Tag)
				c.Parameters = append(c.Parameters, parameter)
				switch parameter.Name {
				case "Code":
					parameter.TypeExpr = field.Type.String()
				case "Codes":
					parameter.TypeExpr = "[]int"
				case "Channels":
					parameter.TypeExpr = "[]string"
				}
			}
		}
		c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}})
		c.RootView.Relations = nil
		for _, option := range configure {
			option(c, types)
		}
		c.RootView.Source.SQL = "SELECT s.tenant,s.account_id,s.region,SUM(s.amount) AS total_spend,COUNT(*) AS order_count FROM report_spend s WHERE s.tenant='acme' ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, \"AND\")).Build(\"AND\")} GROUP BY s.tenant,s.account_id,s.region"
	}}).build(t)
	return &enumPredicateHarness{groupedReportHarness: h, queries: calls}
}
func TestReportNamedEnumsBindNativePredicates(t *testing.T) {
	for _, tc := range []struct {
		name      string
		input     reflect.Type
		filters   map[string]any
		wantCount int
	}{
		{"scalar", reflect.TypeFor[contractEnumScalarInput](), map[string]any{"code": "first"}, 5},
		{"integer-slice", reflect.TypeFor[contractEnumCollectionInput](), map[string]any{"codes": []any{"second"}, "channels": []any{"web"}}, 1},
		{"string-slice", reflect.TypeFor[contractEnumCollectionInput](), map[string]any{"codes": []any{"first", "second"}, "channels": []any{"store"}}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := enumPredicateReportHarness(t, tc.input)
			entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
			require.True(t, ok)
			result, rpc := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"measures": map[string]any{"orderCount": true}, "filters": tc.filters}}})
			require.Nil(t, rpc)
			if result.IsError != nil && *result.IsError {
				plan, _ := h.service.Catalog().Tool("SpendCube")
				scope, scopeErr := plan.Scope(map[string]any{"measures": map[string]any{"orderCount": true}, "filters": tc.filters})
				require.NoError(t, scopeErr)
				defer scope.Close()
				_, cause := h.runtime.InvokeComponent(invocation.WithMCPContext(t.Context(), nil), exec.ComponentRequest{Target: plan.Target(), Providers: scope.Providers()})
				for cause != nil {
					t.Logf("native invocation cause: %T: %v", cause, cause)
					cause = errors.Unwrap(cause)
				}
			}
			require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			var out struct {
				Rows []struct {
					OrderCount int `json:"orderCount"`
				} `json:"rows"`
			}
			require.NoError(t, json.Unmarshal(encoded, &out))
			require.Len(t, out.Rows, 1)
			require.Equal(t, tc.wantCount, out.Rows[0].OrderCount)
			require.EqualValues(t, 1, h.queries.Load(), "one native query per admitted enum predicate")
		})
	}
}

type enumOwnFilters struct {
	Codes    *[]int    `json:"codes,omitempty" mcpEnum:"true" enumAccept:"literal" choice:"2:second"`
	Channels *[]string `json:"channels,omitempty" mcpEnum:"true" enumAccept:"literal" choice:"web"`
}
type enumOwnInput struct {
	Dimensions groupedLinkedDimensions `json:"dimensions,omitempty"`
	Measures   groupedLinkedMeasures   `json:"measures,omitempty"`
	Filters    enumOwnFilters          `json:"filters,omitempty"`
	OrderBy    []string                `json:"orderBy,omitempty"`
	Limit      *int                    `json:"limit,omitempty"`
	Offset     *int                    `json:"offset,omitempty"`
}

func TestReportEnumProjectionPreservesDerivedRestriction(t *testing.T) {
	h := enumPredicateReportHarness(t, reflect.TypeFor[contractEnumCollectionInput](), func(c *spec.Component, types *typecatalog.Catalog) {
		c.Settings.Report.LinkedInputType = "EnumOwnInput"
		require.NoError(t, types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[enumOwnInput](), Name: "EnumOwnInput", PkgPath: "example.com/acme/reporting"}))
	})
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	metadata, _, err := h.service.DescribeTool(t.Context(), entry.Metadata)
	require.NoError(t, err)
	raw, err := json.Marshal(metadata.InputSchema)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "first", "source metadata must not broaden a derived input domain")
	for _, token := range []string{"first", "second"} {
		before := h.queries.Load()
		result, rpc := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"measures": map[string]any{"orderCount": true}, "filters": map[string]any{"codes": []any{token}, "channels": []any{"web"}}}}})
		require.Nil(t, rpc)
		if token == "first" {
			require.True(t, result.IsError != nil && *result.IsError)
			require.Equal(t, before, h.queries.Load())
		} else {
			require.False(t, result.IsError != nil && *result.IsError)
			require.Equal(t, before+1, h.queries.Load())
		}
	}
}
func TestReportEnumCollectionsNullOmissionAndUnknownBeforeSQL(t *testing.T) {
	h := enumPredicateReportHarness(t, reflect.TypeFor[contractEnumCollectionInput]())
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	for _, tc := range []struct {
		name    string
		filters map[string]any
		valid   bool
	}{
		{"omitted", map[string]any{}, true}, {"null", map[string]any{"codes": nil, "channels": nil}, true}, {"unknown-number-token", map[string]any{"codes": []any{"missing"}}, false}, {"unknown-string-token", map[string]any{"channels": []any{"missing"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := h.queries.Load()
			result, rpc := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"measures": map[string]any{"orderCount": true}, "filters": tc.filters}}})
			require.Nil(t, rpc)

			require.Equal(t, !tc.valid, result.IsError != nil && *result.IsError)
			if tc.valid {
				require.Equal(t, before+1, h.queries.Load())
			} else {
				require.Equal(t, before, h.queries.Load())
			}
		})
	}
}

type restEnumCollectionInput contractEnumCollectionInput

func (*restEnumCollectionInput) Contract(ctx context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	if _, mcp := xmcp.LookupContext(ctx); mcp {
		return nil, nil
	}
	return &xcontract.Policy{Name: "fixture-rest-stored-values", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[restEnumCollectionInput]() {
			return field.RemoveTag("choice").RemoveTag("enumAccept").RemoveTag("mcpEnum"), nil
		}
		return nil, nil
	}}, nil
}
func TestReportEnumPredicatesPreserveRESTStoredArraysAndNativePaging(t *testing.T) {
	h := enumPredicateReportHarness(t, reflect.TypeFor[restEnumCollectionInput]())
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/spend/acme/cube", strings.NewReader(`{"measures":{"orderCount":true},"filters":{"codes":[2],"channels":["web"]}}`))
	request.Header.Set("Content-Type", "application/json")
	httpgateway.NewHandler(h.runtime, nil, "report-enum").ServeHTTP(recorder, request)
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"OrderCount":1`)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	before := h.queries.Load()
	result, rpc := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"dimensions": map[string]any{"accountId": true}, "measures": map[string]any{"orderCount": true}, "filters": map[string]any{"codes": []any{"first", "second"}, "channels": []any{"web"}}, "orderBy": []any{"accountId ASC"}, "limit": 1, "offset": 1}}})
	require.Nil(t, rpc)
	require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
	require.Equal(t, before+1, h.queries.Load())
	raw, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	var out struct {
		Rows []struct {
			AccountID  int `json:"accountId"`
			OrderCount int `json:"orderCount"`
		} `json:"rows"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	require.Len(t, out.Rows, 1)
	require.Equal(t, 2, out.Rows[0].AccountID)
	require.Equal(t, 1, out.Rows[0].OrderCount)
}
