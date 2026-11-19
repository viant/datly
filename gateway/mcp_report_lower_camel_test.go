package gateway

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/tagly/format/text"
)

func TestRouter_MCPReportLowerCamelArgumentsReachCubeBody(t *testing.T) {
	resource := view.EmptyResource()
	rootView := view.NewView("performance", "PERFORMANCE")
	rootView.Groupable = true
	rootView.Columns = []*view.Column{
		view.NewColumn("PublisherID", "int", reflect.TypeOf(0), false),
		view.NewColumn("TotalSpend", "float64", reflect.TypeOf(float64(0)), false),
	}
	rootView.Columns[0].Groupable = true
	rootView.Columns[1].Aggregate = true
	for _, column := range rootView.Columns {
		require.NoError(t, column.Init(&repositoryReportTestResource{}, text.CaseFormatUndefined, false))
	}
	rootView.SetResource(resource)
	resource.AddViews(rootView)

	inputType, err := state.NewType(state.WithParameters(state.Parameters{
		&state.Parameter{
			Name:       "audienceIDs",
			In:         state.NewQueryLocation("audience_id"),
			Schema:     state.NewSchema(reflect.TypeOf([]int{})),
			Predicates: []*extension.PredicateConfig{{Name: "ByAudience"}},
		},
	}), state.WithResource(&repositoryReportTestResource{}))
	require.NoError(t, err)
	inputType.Name = "PerformanceInput"

	original := &repository.Component{
		Path:     contract.Path{Method: http.MethodGet, URI: "/v1/api/performance"},
		Meta:     contract.Meta{Name: "performance"},
		View:     rootView,
		Report:   &repository.Report{Enabled: true},
		Contract: contract.Contract{Input: contract.Input{Type: *inputType}},
	}
	reportComponent, err := repository.BuildReportComponent(nil, original)
	require.NoError(t, err)

	var actualBody string
	route := &Route{
		Path: &reportComponent.Path,
		Handler: func(_ context.Context, response http.ResponseWriter, request *http.Request) {
			payload, _ := io.ReadAll(request.Body)
			actualBody = string(payload)
			response.WriteHeader(http.StatusOK)
			_, _ = response.Write([]byte(`{"status":"ok","data":[]}`))
		},
	}

	result, rpcErr := (&Router{}).mcpToolCallHandler(reportComponent, route)(context.Background(), &schema.CallToolRequest{
		Params: schema.CallToolRequestParams{Arguments: map[string]interface{}{
			"dimensions": map[string]interface{}{"publisherID": true},
			"measures":   map[string]interface{}{"totalSpend": true},
			"filters":    map[string]interface{}{"audienceIDs": []interface{}{7396187.0}},
			"orderBy":    []interface{}{"total_spend:desc"},
			"limit":      200.0,
		}},
	})

	require.Nil(t, rpcErr)
	require.NotNil(t, result)
	assert.JSONEq(t, `{
		"dimensions":{"publisherID":true},
		"measures":{"totalSpend":true},
		"filters":{"audienceIDs":[7396187]},
		"orderBy":["total_spend:desc"],
		"limit":200
	}`, actualBody)
}
