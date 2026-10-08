package report

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
)

type nullDateInput struct {
	From *time.Time        `format:"dateFormat=YYYY-MM-DD"`
	To   *time.Time        `format:"dateFormat=YYYY-MM-DD"`
	Has  *nullDatePresence `setMarker:"true" json:"-"`
}

type nullDatePresence struct {
	From bool
	To   bool
}

type nullDateCubeInput struct {
	Dimensions groupedLinkedDimensions `json:"dimensions,omitempty"`
	Measures   groupedLinkedMeasures   `json:"measures,omitempty"`
	Filters    struct {
		From *any              `json:"from,omitempty"`
		To   *any              `json:"to,omitempty"`
		Has  *nullDatePresence `setMarker:"true" json:"-"`
	} `json:"filters,omitempty"`
	OrderBy []string `json:"orderBy,omitempty"`
	Limit   *int     `json:"limit,omitempty"`
	Offset  *int     `json:"offset,omitempty"`
}

func TestCubeNullDateBoundariesHTTPMCP(t *testing.T) {
	t.Run("dynamic", func(t *testing.T) { testCubeNullDateBoundaries(t, false) })
	t.Run("linked", func(t *testing.T) { testCubeNullDateBoundaries(t, true) })
}

func testCubeNullDateBoundaries(t *testing.T, linked bool) {
	h := (groupedReportHarnessConfig{
		input: reflect.TypeFor[nullDateInput](),
		source: func(component *spec.Component, types *typecatalog.Catalog) {
			if linked {
				component.Settings.Report.LinkedInputType = "NullDateCubeInput"
				require.NoError(t, types.Register(typecatalog.TypeOriginPackage, &x.Type{
					PkgPath: component.Key.Scope, Name: "NullDateCubeInput", Type: reflect.TypeFor[nullDateCubeInput](),
				}))
			}
			component.Parameters = []*spec.Parameter{
				{Name: "From", Source: spec.BindSource{Kind: "form", Name: "from"}},
				{Name: "To", Source: spec.BindSource{Kind: "form", Name: "to"}},
				{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
			}
			component.RootView.Relations = nil
			component.RootView.Source.SQL = `SELECT s.tenant, s.account_id, s.region,
SUM(s.amount) AS total_spend, COUNT(*) AS order_count
FROM report_spend s
WHERE 1 = 1
#if($Has.From) AND DATE('2026-10-08') >= DATE($From) #end
#if($Has.To) AND DATE('2026-10-08') <= DATE($To) #end
GROUP BY s.tenant, s.account_id, s.region`
		},
	}).build(t)
	for _, tc := range []struct {
		name, filters string
		count         int
		invalid       bool
	}{
		{name: "omitted", filters: `{}`, count: 9},
		{name: "null from", filters: `{"from":null}`, count: 9},
		{name: "null to", filters: `{"to":null}`, count: 9},
		{name: "both null", filters: `{"from":null,"to":null}`, count: 9},
		{name: "lower boundary", filters: `{"from":"2026-10-08","to":null}`, count: 9},
		{name: "upper boundary", filters: `{"from":null,"to":"2026-10-08"}`, count: 9},
		{name: "excluded lower", filters: `{"from":"2026-10-09","to":null}`},
		{name: "excluded upper", filters: `{"from":null,"to":"2026-10-07"}`},
		{name: "invalid", filters: `{"from":"not-a-date"}`, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"measures":{"orderCount":true},"filters":` + tc.filters + `}`
			check := func(t *testing.T, raw []byte) {
				t.Helper()
				var output groupedSpendOutput
				require.NoError(t, json.Unmarshal(raw, &output))
				require.Len(t, output.Rows, 1)
				require.Equal(t, tc.count, output.Rows[0].OrderCount)
			}
			t.Run("HTTP", func(t *testing.T) {
				request := httptest.NewRequest("POST", "/spend/acme/cube", strings.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(response, request)
				if tc.invalid {
					require.Equal(t, 400, response.Code, response.Body.String())
					return
				}
				require.Equal(t, 200, response.Code, response.Body.String())
				check(t, response.Body.Bytes())
			})
			t.Run("MCP", func(t *testing.T) {
				var args map[string]any
				require.NoError(t, json.Unmarshal([]byte(body), &args))
				tool, ok := h.service.Registry().ToolRegistry.Get("SpendCube")
				require.True(t, ok)
				result, err := tool.Handler(context.Background(), &schema.CallToolRequest{
					Method: schema.MethodToolsCall,
					Params: schema.CallToolRequestParams{Name: "SpendCube", Arguments: args},
				})
				if tc.invalid {
					require.True(t, err != nil || result != nil && result.IsError != nil && *result.IsError)
					return
				}
				require.Nil(t, err)
				require.NotNil(t, result)
				require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
				check(t, []byte(result.Content[0].(schema.TextContent).Text))
			})
		})
	}
}

func TestFilterSourceValueNullAndZero(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   any
		present bool
	}{
		{name: "null"},
		{name: "typed null", value: (*time.Time)(nil)},
		{name: "nil slice", value: []int(nil)},
		{name: "false", value: false, present: true},
		{name: "zero", value: 0, present: true},
		{name: "empty string", value: "", present: true},
		{name: "empty slice", value: []int{}, present: true},
		{name: "date", value: "2026-10-08", present: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := struct{ Value *any }{Value: &tc.value}
			value, present, err := filterSourceValue(reflect.ValueOf(input), []int{0}, reflect.TypeFor[any]())
			require.NoError(t, err)
			require.Equal(t, tc.present, present)
			if present {
				require.Equal(t, tc.value, value.Interface())
			}
		})
	}
}
