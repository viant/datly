package report

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

type enumSourceInput struct {
	Integer int64
	Small   int64
	Real    float64
	Text    string
	Flag    bool
}

func TestCubeChoicesAreMetadataHTTPMCP(t *testing.T) {
	definitions := []struct {
		name, wire, typ, defaultValue string
		choices                       []any
		valid, invalid                any
	}{
		{"Integer", "integer", "integer", "9007199254740993", []any{json.Number("9007199254740993"), json.Number("9007199254740994")}, json.Number("9007199254740994"), json.Number("9007199254740995")},
		{"Small", "small", "integer", "1", []any{json.Number("1")}, 1, 1.5},
		{"Real", "real", "number", "1.25", []any{json.Number("1.25"), json.Number("2.5")}, 2.5, 3.5},
		{"Text", "text", "string", "one", []any{"one", "two"}, "two", "other"},
		{"Flag", "flag", "boolean", "false", []any{false}, false, true},
	}
	h := (groupedReportHarnessConfig{input: reflect.TypeFor[enumSourceInput](), source: func(c *spec.Component, _ *typecatalog.Catalog) {
		c.Parameters = nil
		for _, d := range definitions {
			value := d.defaultValue
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: d.name, Source: spec.BindSource{Kind: "query", Name: d.wire}, Value: &value, WireSchema: &spec.WireSchema{Type: d.typ, Enum: d.choices}})
		}
		c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}})
		c.RootView.Relations = nil
		c.RootView.Source.SQL = "SELECT s.tenant,s.account_id,s.region,SUM(s.amount) AS total_spend,COUNT(*) AS order_count FROM report_spend s WHERE s.tenant='acme' GROUP BY s.tenant,s.account_id,s.region"
	}}).build(t)
	tool, ok := h.service.Catalog().Tool("SpendCube")
	require.True(t, ok)
	metadata, _ := json.Marshal(tool.Metadata().InputSchema.Properties["filters"])
	require.Contains(t, string(metadata), "9007199254740993")
	var properties struct {
		Properties map[string]struct {
			Enum []any `json:"enum"`
		}
	}
	decoder := json.NewDecoder(strings.NewReader(string(metadata)))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&properties))
	for _, d := range definitions {
		require.Equal(t, append(append([]any(nil), d.choices...), nil), properties.Properties[d.wire].Enum)
	}
	native, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
	require.True(t, ok)
	for _, d := range definitions {
		for _, scenario := range []struct {
			value   any
			invalid bool
		}{{d.valid, false}, {d.invalid, true}, {nil, false}} {
			args := map[string]any{"measures": map[string]any{"orderCount": true}, "filters": map[string]any{d.wire: scenario.value}}
			body, err := json.Marshal(args)
			require.NoError(t, err)
			response := httptest.NewRecorder()
			request := httptest.NewRequest("POST", "/spend/acme/cube", strings.NewReader(string(body)))
			request.Header.Set("Content-Type", "application/json")
			httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(response, request)
			// Choices alone are metadata; only ordinary scalar conversion
			// errors remain binding failures without an explicit validator.
			typeInvalid := scenario.invalid && d.name == "Small"
			expected := 200
			if typeInvalid {
				expected = 400
			}
			require.Equal(t, expected, response.Code, "%s: %s", d.name, response.Body.String())
			result, protocolErr := native.Handler(context.Background(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: args}})
			if typeInvalid {
				require.True(t, protocolErr != nil || result != nil && result.IsError != nil && *result.IsError, d.name)
			} else {
				require.Nil(t, protocolErr)
				require.NotNil(t, result)
				require.False(t, result.IsError != nil && *result.IsError, "%s: %+v", d.name, result)
			}
		}
	}
	for _, protocol := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		t.Run("wire_"+protocol, func(t *testing.T) {
			client := mcpclient.New(t, h.service, protocol)
			for _, tc := range []struct {
				value   string
				invalid bool
			}{{"9007199254740993", false}, {"9007199254740994", false}, {"9007199254740992", false}, {"9007199254740995", false}} {
				// The real client serializes a JSON numeric token. The loopback MCP
				// server retains native typed conversion; choice metadata alone
				// must not add runtime domain rejection.
				result, err := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "SpendCube", Arguments: map[string]any{"measures": map[string]any{"orderCount": true}, "filters": map[string]any{"integer": json.Number(tc.value)}}})
				failed := err != nil || result == nil || result.IsError != nil && *result.IsError
				if failed != tc.invalid {
					t.Fatalf("wire number %s invalid=%v: %v %+v", tc.value, tc.invalid, err, result)
				}
			}
		})
	}

}
