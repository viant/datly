package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
)

func TestRouter_buildToolInputType_FlattensAnonymousBody(t *testing.T) {
	bodyType := reflect.StructOf([]reflect.StructField{
		{
			Name: "Dimensions",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "AccountId", Type: reflect.TypeOf(false), Tag: `json:"accountId,omitempty" desc:"Account identifier"`},
			}),
			Tag: `json:"dimensions,omitempty"`,
		},
		{
			Name: "Measures",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "TotalId", Type: reflect.TypeOf(false), Tag: `json:"totalId,omitempty" desc:"Total identifier"`},
			}),
			Tag: `json:"measures,omitempty"`,
		},
		{
			Name: "Filters",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "VendorIDs", Type: reflect.TypeOf([]int{}), Tag: `json:"vendorIDs,omitempty" desc:"Vendor IDs"`},
			}),
			Tag: `json:"filters,omitempty"`,
		},
		{Name: "OrderBy", Type: reflect.TypeOf([]string{}), Tag: `json:"orderBy,omitempty"`},
	})
	bodyParam := state.NewParameter("Report", state.NewBodyLocation(""), state.WithParameterSchema(state.NewSchema(bodyType)))
	bodyParam.Tag = `anonymous:"true"`
	component := &repository.Component{
		Path: contract.Path{Method: "POST", URI: "/v1/api/dev/vendors-grouping/report"},
		View: &view.View{},
		Contract: contract.Contract{
			Input: contract.Input{
				Type: state.Type{Parameters: state.Parameters{bodyParam}},
			},
		},
	}

	rType := (&Router{}).buildToolInputType(component)
	require.Equal(t, reflect.Struct, rType.Kind())
	_, ok := rType.FieldByName("Report")
	assert.False(t, ok)
	for _, name := range []string{"Dimensions", "Measures", "Filters", "OrderBy"} {
		_, ok = rType.FieldByName(name)
		assert.True(t, ok, name)
	}
}

func TestAnonymousBodyArgumentValue_UsesJSONFieldNames(t *testing.T) {
	bodyType := reflect.StructOf([]reflect.StructField{
		{Name: "Dimensions", Type: reflect.StructOf([]reflect.StructField{{Name: "AccountId", Type: reflect.TypeOf(false), Tag: `json:"accountId,omitempty"`}}), Tag: `json:"dimensions,omitempty"`},
		{Name: "Measures", Type: reflect.StructOf([]reflect.StructField{{Name: "TotalId", Type: reflect.TypeOf(false), Tag: `json:"totalId,omitempty"`}}), Tag: `json:"measures,omitempty"`},
		{Name: "Filters", Type: reflect.StructOf([]reflect.StructField{{Name: "VendorIDs", Type: reflect.TypeOf([]int{}), Tag: `json:"vendorIDs,omitempty"`}}), Tag: `json:"filters,omitempty"`},
		{Name: "OrderBy", Type: reflect.TypeOf([]string{}), Tag: `json:"orderBy,omitempty"`},
		{Name: "Limit", Type: reflect.TypeOf((*int)(nil)), Tag: `json:"limit,omitempty"`},
	})

	value := anonymousBodyArgumentValue(map[string]interface{}{
		"Dimensions": map[string]interface{}{"AccountId": true},
		"Measures":   map[string]interface{}{"TotalId": true},
		"Filters":    map[string]interface{}{"VendorIDs": []interface{}{1.0, 2.0, 3.0}},
		"OrderBy":    []interface{}{"accountId"},
	}, bodyType)

	data, err := json.Marshal(value)
	require.NoError(t, err)
	assert.JSONEq(t, `{
	  "dimensions":{"AccountId":true},
	  "measures":{"TotalId":true},
	  "filters":{"VendorIDs":[1,2,3]},
	  "orderBy":["accountId"]
	}`, string(data))
}

func TestRouter_addAuthTokenIfPresent_AddsBearerToken(t *testing.T) {
	router := &Router{}
	req, err := http.NewRequest(http.MethodPost, "http://localhost/v1/api/dev/vendors-grouping/report", nil)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), authorization.TokenKey, &authorization.Token{Token: "abc123"})
	router.addAuthTokenIfPresent(ctx, req)

	assert.Equal(t, "Bearer abc123", req.Header.Get("Authorization"))
}

func TestRouter_mcpToolCallHandler_PassesAuthorizationToReportRoute(t *testing.T) {
	bodyType := reflect.StructOf([]reflect.StructField{
		{
			Name: "Dimensions",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "AccountId", Type: reflect.TypeOf(false), Tag: `json:"accountId,omitempty"`},
			}),
			Tag: `json:"dimensions,omitempty"`,
		},
		{
			Name: "Measures",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "TotalId", Type: reflect.TypeOf(false), Tag: `json:"totalId,omitempty"`},
			}),
			Tag: `json:"measures,omitempty"`,
		},
		{
			Name: "Filters",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "VendorIDs", Type: reflect.TypeOf([]int{}), Tag: `json:"vendorIDs,omitempty"`},
			}),
			Tag: `json:"filters,omitempty"`,
		},
		{Name: "OrderBy", Type: reflect.TypeOf([]string{}), Tag: `json:"orderBy,omitempty"`},
	})
	bodyParam := state.NewParameter("Report", state.NewBodyLocation(""), state.WithParameterSchema(state.NewSchema(bodyType)))
	bodyParam.Tag = `anonymous:"true"`
	component := &repository.Component{
		Path: contract.Path{Method: http.MethodPost, URI: "/v1/api/dev/vendors-grouping/report"},
		Contract: contract.Contract{
			Input: contract.Input{
				Type: state.Type{Parameters: state.Parameters{bodyParam}},
			},
		},
	}

	var actualAuth string
	var actualBody string
	route := &Route{
		Path: &contract.Path{Method: http.MethodPost, URI: "/v1/api/dev/vendors-grouping/report"},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			actualAuth = req.Header.Get("Authorization")
			if req.Body != nil {
				payload, _ := io.ReadAll(req.Body)
				actualBody = string(payload)
			}
			response.WriteHeader(http.StatusOK)
			_, _ = response.Write([]byte(`{"ok":true}`))
		},
	}

	handler := (&Router{}).mcpToolCallHandler(component, route)
	ctx := context.WithValue(context.Background(), authorization.TokenKey, &authorization.Token{Token: "jwt-token"})
	result, rpcErr := handler(ctx, &schema.CallToolRequest{
		Params: schema.CallToolRequestParams{
			Arguments: map[string]interface{}{
				"Dimensions": map[string]interface{}{"AccountId": true},
				"Measures":   map[string]interface{}{"TotalId": true},
				"Filters":    map[string]interface{}{"VendorIDs": []interface{}{1.0, 2.0}},
				"OrderBy":    []interface{}{"accountId"},
			},
		},
	})

	require.Nil(t, rpcErr)
	require.NotNil(t, result)
	assert.Equal(t, "Bearer jwt-token", actualAuth)
	assert.JSONEq(t, `{
		"dimensions":{"AccountId":true},
		"measures":{"TotalId":true},
		"filters":{"VendorIDs":[1,2]},
		"orderBy":["accountId"]
	}`, actualBody)
}
