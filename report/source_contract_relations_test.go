package report

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	xcontract "github.com/viant/xdatly/contract"
)

type sourceRelationPolicyRow graphSpendRow
type sourceRelationPolicyOutput struct{ Rows []*sourceRelationPolicyRow }

func (*sourceRelationPolicyRow) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Mode string `parameter:",kind=header,in=X-Report-Relation"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	return &xcontract.Policy{Name: "source-relation:" + state.Mode, Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if state.Mode == "holder" && parent == reflect.TypeFor[sourceRelationPolicyRow]() && field.Name == "Label" || state.Mode == "leaf" && parent == reflect.TypeFor[graphSpendLabel]() && field.Name == "Label" {
			return field.SetTag("mcp", "-"), nil
		}
		return nil, nil
	}}, nil
}
func TestSourceContractRejectsProtectedRelationHolderAndLeafBeforeQuery(t *testing.T) {
	h := (groupedReportHarnessConfig{output: reflect.TypeFor[sourceRelationPolicyOutput](), report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
		s.Compose = &spec.CubeComposeSettings{Enabled: true, ProjectionRelations: true}
	}}, source: func(c *spec.Component, _ *typecatalog.Catalog) {
		relation := c.RootView.Relations[0]
		relation.Name = "Label"
		relation.Holder = "Label"
		relation.Cardinality = spec.CardinalityOne
		relation.View.Source.SQL = "SELECT tenant,account_id,label,'private' AS secret FROM report_detail WHERE tenant=$Tenant AND $COLUMN_IN"
	}}).build(t)
	entry, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	require.True(t, ok)
	for _, mode := range []string{"holder", "leaf"} {
		t.Run(mode, func(t *testing.T) {
			ctx := invocation.WithHTTPHeaders(t.Context(), http.Header{"X-Report-Relation": {mode}})
			result, protocolErr := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: graphComposeRequest()}})
			require.Nil(t, protocolErr)
			require.True(t, result.IsError != nil && *result.IsError)
			require.Zero(t, h.codec.calls.Load())
			raw, err := json.Marshal(result)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "alpha")
		})
	}
}
