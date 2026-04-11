package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	serverproto "github.com/viant/mcp-protocol/server"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type repositoryReportTestResource struct{}

func (r *repositoryReportTestResource) LookupParameter(name string) (*state.Parameter, error) {
	return nil, nil
}
func (r *repositoryReportTestResource) AppendParameter(parameter *state.Parameter) {}
func (r *repositoryReportTestResource) ViewSchema(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *repositoryReportTestResource) ViewSchemaPointer(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *repositoryReportTestResource) LookupType() xreflect.LookupType { return nil }
func (r *repositoryReportTestResource) LoadText(ctx context.Context, URL string) (string, error) {
	return "", nil
}
func (r *repositoryReportTestResource) Codecs() *codec.Registry                  { return codec.New() }
func (r *repositoryReportTestResource) CodecOptions() *codec.Options             { return codec.NewOptions(nil) }
func (r *repositoryReportTestResource) ExpandSubstitutes(value string) string    { return value }
func (r *repositoryReportTestResource) ReverseSubstitutes(value string) string   { return value }
func (r *repositoryReportTestResource) EmbedFS() *embed.FS                       { return nil }
func (r *repositoryReportTestResource) SetFSEmbedder(embedder *state.FSEmbedder) {}

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

func TestAnonymousBodyArgumentValue_AcceptsJSONStyleTopLevelArgumentNames(t *testing.T) {
	bodyType := reflect.StructOf([]reflect.StructField{
		{Name: "Dimensions", Type: reflect.StructOf([]reflect.StructField{{Name: "AdOrderId", Type: reflect.TypeOf(false), Tag: `json:"adOrderId,omitempty"`}}), Tag: `json:"dimensions,omitempty"`},
		{Name: "Measures", Type: reflect.StructOf([]reflect.StructField{{Name: "Bids", Type: reflect.TypeOf(false), Tag: `json:"bids,omitempty"`}}), Tag: `json:"measures,omitempty"`},
	})

	value := anonymousBodyArgumentValue(map[string]interface{}{
		"dimensions": map[string]interface{}{"adOrderId": true},
		"measures":   map[string]interface{}{"bids": true},
	}, bodyType)

	data, err := json.Marshal(value)
	require.NoError(t, err)
	assert.JSONEq(t, `{
	  "dimensions":{"adOrderId":true},
	  "measures":{"bids":true}
	}`, string(data))
}

func TestToolArgumentValue_AcceptsSnakeCaseAliases(t *testing.T) {
	parameter := state.NewParameter("audience_id", state.NewQueryLocation("audience_id"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf([]int{}))))

	testCases := []struct {
		name      string
		arguments map[string]interface{}
	}{
		{
			name:      "title alias",
			arguments: map[string]interface{}{"Audience_id": []interface{}{7180287.0}},
		},
		{
			name:      "raw query name",
			arguments: map[string]interface{}{"audience_id": []interface{}{7180287.0}},
		},
		{
			name:      "pascal case alias",
			arguments: map[string]interface{}{"AudienceId": []interface{}{7180287.0}},
		},
		{
			name:      "lower camel alias",
			arguments: map[string]interface{}{"audienceId": []interface{}{7180287.0}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			value := toolArgumentValue(parameter, testCase.arguments)
			require.Equal(t, []interface{}{7180287.0}, value)
		})
	}
}

func TestToolArgumentValue_AcceptsSelectorAliases(t *testing.T) {
	parameter := &state.Parameter{
		Name:   "Limit",
		In:     state.NewQueryLocation("limit"),
		Schema: state.NewSchema(reflect.TypeOf(0)),
	}

	testCases := []struct {
		name      string
		arguments map[string]interface{}
		want      interface{}
	}{
		{
			name:      "exported field name",
			arguments: map[string]interface{}{"Limit": 25.0},
			want:      25.0,
		},
		{
			name:      "public query name",
			arguments: map[string]interface{}{"limit": 25.0},
			want:      25.0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			value := toolArgumentValue(parameter, testCase.arguments)
			require.Equal(t, testCase.want, value)
		})
	}
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

func TestRouter_mcpToolCallHandler_MapsComponentAndSelectorArgumentsToHTTPQuery(t *testing.T) {
	component := &repository.Component{
		Path: contract.Path{Method: http.MethodGet, URI: "/v1/api/steward/metadata/ad_profile"},
		Contract: contract.Contract{
			Input: contract.Input{
				Type: state.Type{Parameters: state.Parameters{
					state.NewParameter("AudienceId", state.NewFormLocation("audience_id"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf([]int{})))),
					state.NewParameter("Limit", state.NewQueryLocation("lm_limit"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(0)))),
				}},
			},
		},
	}

	var actualQuery string
	route := &Route{
		Path: &contract.Path{Method: http.MethodGet, URI: "/v1/api/steward/metadata/ad_profile"},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			actualQuery = req.URL.RawQuery
			response.WriteHeader(http.StatusOK)
			_, _ = response.Write([]byte(`{"ok":true}`))
		},
	}

	handler := (&Router{}).mcpToolCallHandler(component, route)
	result, rpcErr := handler(context.Background(), &schema.CallToolRequest{
		Params: schema.CallToolRequestParams{
			Arguments: map[string]interface{}{
				"AudienceId": []interface{}{7180287.0},
				"limit":      25.0,
			},
		},
	})

	require.Nil(t, rpcErr)
	require.NotNil(t, result)
	values, err := url.ParseQuery(actualQuery)
	require.NoError(t, err)
	assert.Equal(t, "7180287", values.Get("audience_id"))
	assert.Equal(t, "25", values.Get("limit"))
	assert.Empty(t, values.Get("AudienceId"))
	assert.Empty(t, values.Get("lm_limit"))
}

func TestRouter_newToolHTTPRequest_SetsJSONContentTypeForBody(t *testing.T) {
	req, rpcErr := (&Router{}).newToolHTTPRequest(http.MethodPost, "http://localhost/v1/api/dev/vendors-grouping/report", strings.NewReader(`{"dimensions":{"accountId":true}}`))
	require.Nil(t, rpcErr)
	require.NotNil(t, req)
	assert.Equal(t, "application/json", req.Header.Get("Content-Type"))
}

func TestRouter_buildToolsIntegration_RegistersCubeTool(t *testing.T) {
	bodyType := reflect.StructOf([]reflect.StructField{
		{
			Name: "Dimensions",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "AccountId", Type: reflect.TypeOf(false), Tag: `json:"accountId,omitempty" desc:"Account identifier"`},
			}),
			Tag: `json:"dimensions,omitempty" desc:"Selected grouping dimensions"`,
		},
		{
			Name: "Measures",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "TotalId", Type: reflect.TypeOf(false), Tag: `json:"totalId,omitempty" desc:"Total identifier"`},
			}),
			Tag: `json:"measures,omitempty" desc:"Selected aggregate measures"`,
		},
		{
			Name: "Filters",
			Type: reflect.StructOf([]reflect.StructField{
				{Name: "VendorIDs", Type: reflect.TypeOf([]int{}), Tag: `json:"vendorIDs,omitempty" desc:"Vendor IDs to include"`},
			}),
			Tag: `json:"filters,omitempty" desc:"Report filters derived from original predicate parameters"`,
		},
		{Name: "OrderBy", Type: reflect.TypeOf([]string{}), Tag: `json:"orderBy,omitempty"`},
	})
	bodyParam := state.NewParameter("Report", state.NewBodyLocation(""), state.WithParameterSchema(state.NewSchema(bodyType)))
	bodyParam.Tag = `anonymous:"true"`
	component := &repository.Component{
		Path: contract.Path{Method: http.MethodPost, URI: "/v1/api/dev/vendors-grouping/report"},
		View: &view.View{Name: "vendor"},
		Contract: contract.Contract{
			Input: contract.Input{
				Type: state.Type{Parameters: state.Parameters{bodyParam}},
			},
		},
	}
	provider := repository.NewProvider(
		contract.Path{Method: http.MethodPost, URI: "/v1/api/dev/vendors-grouping/report"},
		&version.Control{},
		func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) {
			return component, nil
		},
	)
	route := &Route{
		Path: &contract.Path{Method: http.MethodPost, URI: "/v1/api/dev/vendors-grouping/report"},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			response.WriteHeader(http.StatusOK)
		},
	}
	registry := serverproto.NewRegistry()
	router := &Router{mcpRegistry: registry}

	err := router.buildToolsIntegration(&dpath.Item{}, &dpath.Path{
		Path: contract.Path{Method: http.MethodPost, URI: "/v1/api/dev/vendors-grouping/report"},
		Meta: contract.Meta{Name: "vendors grouping cube", Description: "Vendor grouping cube"},
		ModelContextProtocol: contract.ModelContextProtocol{
			MCPTool: true,
		},
		View: &dpath.ViewRef{Ref: "vendor"},
	}, route, provider)
	require.NoError(t, err)

	tools := registry.ListRegisteredTools()
	require.Len(t, tools, 1)
	tool := tools[0]
	assert.Equal(t, "vendorsgroupingcube", tool.Name)
	require.Contains(t, tool.InputSchema.Properties, "dimensions")
	require.Contains(t, tool.InputSchema.Properties, "measures")
	require.Contains(t, tool.InputSchema.Properties, "filters")
}

func TestRouter_buildToolCallResult_DecompressesGzipBody(t *testing.T) {
	var compressed bytes.Buffer
	gzw := gzip.NewWriter(&compressed)
	_, err := gzw.Write([]byte(`{"data":{"rows":[1,2,3]},"status":"ok"}`))
	require.NoError(t, err)
	require.NoError(t, gzw.Close())

	writer := proxy.NewWriter()
	writer.Code = http.StatusOK
	writer.HeaderMap.Set("Content-Type", "application/json")
	writer.HeaderMap.Set("Content-Encoding", "gzip")
	_, err = writer.Body.Write(compressed.Bytes())
	require.NoError(t, err)

	result := (&Router{}).buildToolCallResult(writer, "http://localhost/test", http.MethodPost)
	require.NotNil(t, result)
	require.Len(t, result.Content, 1)

	text, ok := result.Content[0].(schema.TextContent)
	require.True(t, ok)
	assert.JSONEq(t, `{"data":{"rows":[1,2,3]},"status":"ok"}`, text.Text)
	assert.Equal(t, map[string]interface{}{
		"data":   map[string]interface{}{"rows": []interface{}{float64(1), float64(2), float64(3)}},
		"status": "ok",
	}, result.StructuredContent)
}

func TestRouter_buildToolInputType_UsesParameterMetadataTags(t *testing.T) {
	fieldParam := state.NewParameter("Field", state.NewPathLocation("field"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(""))))
	fieldParam.Description = "Targeting field key."
	fieldParam.Example = "IRIS_SEGMENTS"

	operationParam := state.NewParameter("Operation", state.NewPathLocation("operation"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(""))))
	operationParam.Description = "Targeting tree operation."
	operationParam.Example = "children"

	component := &repository.Component{
		Path: contract.Path{Method: http.MethodPost, URI: "/v1/api/platform/targeting/tree/{field}/{operation}"},
		View: &view.View{},
		Contract: contract.Contract{
			Input: contract.Input{
				Type: state.Type{Parameters: state.Parameters{fieldParam, operationParam}},
			},
		},
	}

	rType := (&Router{}).buildToolInputType(component)
	field, ok := rType.FieldByName("Field")
	require.True(t, ok)
	assert.Equal(t, "Targeting field key.", field.Tag.Get("description"))
	assert.Equal(t, "IRIS_SEGMENTS", field.Tag.Get("example"))

	operation, ok := rType.FieldByName("Operation")
	require.True(t, ok)
	assert.Equal(t, "Targeting tree operation.", operation.Tag.Get("description"))
	assert.Equal(t, "children", operation.Tag.Get("example"))
}

func TestRouter_applyParamToRequest_UsesPublicSelectorQueryNames(t *testing.T) {
	router := &Router{}
	values := url.Values{}
	param := state.NewParameter("Limit", state.NewQueryLocation("lm_limit"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(0))))

	baseURL, body, rpcErr := router.applyParamToRequest("http://localhost/test", values, param, 20, map[string]bool{}, map[string]bool{}, nil)
	require.Nil(t, rpcErr)
	assert.Equal(t, "http://localhost/test", baseURL)
	assert.Nil(t, body)
	assert.Equal(t, "20", values.Get("limit"))
	assert.Empty(t, values.Get("lm_limit"))
}

func TestRouter_buildToolInputType_UsesBuiltReportComponentParameters(t *testing.T) {
	resource := view.EmptyResource()
	rootView := view.NewView("vendor", "VENDOR")
	rootView.Groupable = true
	rootView.Columns = []*view.Column{
		view.NewColumn("AccountID", "int", reflect.TypeOf(0), false),
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
		&state.Parameter{Name: "vendorIDs", In: state.NewQueryLocation("vendorIDs"), Schema: state.NewSchema(reflect.TypeOf([]int{})), Predicates: []*extension.PredicateConfig{{Name: "ByVendor"}}, Description: "Vendor IDs to include"},
	}), state.WithResource(&repositoryReportTestResource{}))
	require.NoError(t, err)
	inputType.Name = "VendorInput"

	component := &repository.Component{
		Path:   contract.Path{Method: http.MethodGet, URI: "/v1/api/vendors"},
		Meta:   contract.Meta{Name: "vendors"},
		View:   rootView,
		Report: &repository.Report{Enabled: true},
		Contract: contract.Contract{
			Input: contract.Input{Type: *inputType},
		},
	}

	reportComponent, err := repository.BuildReportComponent(nil, component)
	require.NoError(t, err)
	require.NotNil(t, reportComponent)
	require.Len(t, reportComponent.Input.Type.Parameters, 1)

	rType := (&Router{}).buildToolInputType(reportComponent)
	require.Equal(t, reflect.Struct, rType.Kind())
	_, ok := rType.FieldByName("Report")
	assert.False(t, ok)
	for _, name := range []string{"Dimensions", "Measures", "Filters", "OrderBy", "Limit", "Offset"} {
		_, ok = rType.FieldByName(name)
		assert.True(t, ok, name)
	}
}
