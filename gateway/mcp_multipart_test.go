package gateway

import (
	"context"
	"encoding/base64"
	"io"
	"mime/multipart"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view/state"
	"github.com/viant/mcp-protocol/schema"
)

type mcpMultipartJSONPayload struct {
	CampaignID int `json:"campaignId"`
}

func mcpMultipartComponent() (*repository.Component, []*state.Parameter) {
	files := state.NewParameter("Files", state.NewFormLocation("file"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf([]*multipart.FileHeader{}))))
	action := state.NewParameter("Action", state.NewFormLocation("uploadAction"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(""))))
	data := state.NewParameter("Data", state.NewBodyLocation("data"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(mcpMultipartJSONPayload{}))))
	parameters := []*state.Parameter{files, action, data}
	component := &repository.Component{Contract: contract.Contract{Input: contract.Input{Type: state.Type{Parameters: parameters}}}}
	return component, parameters
}

func TestMCPMultipartToolSchemaOffersBlobAlongsideJSON(t *testing.T) {
	component, _ := mcpMultipartComponent()
	inputType := (&Router{}).buildToolInputTypeForPath(component, nil)
	toolSchema := schema.ToolInputSchema{}
	require.NoError(t, toolSchema.Load(reflect.New(inputType).Interface()))

	files := toolSchema.Properties["Files"]
	require.Equal(t, "array", files["type"])
	items, ok := files["items"].(map[string]interface{})
	require.True(t, ok)
	properties, ok := items["properties"].(schema.ToolInputSchemaProperties)
	require.True(t, ok)
	assert.Contains(t, properties, "data")
	assert.Contains(t, properties, "filename")
	assert.Contains(t, properties, "mimeType")
	assert.Contains(t, toolSchema.Properties, "Data", "JSON body remains available on hybrid routes")
	assert.NotContains(t, toolSchema.Required, "Data", "hybrid route must permit blob-only calls")
}

func TestMCPMultipartBodyCarriesBlobFormAndJSON(t *testing.T) {
	_, parameters := mcpMultipartComponent()
	body, rpcErr := buildMCPMultipartBody(parameters, map[string]interface{}{
		"Files": []interface{}{map[string]interface{}{
			"data":     base64.StdEncoding.EncodeToString([]byte("campaign workbook")),
			"filename": "campaign.xlsx",
			"mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		}},
		"Action": "campaign",
		"Data":   map[string]interface{}{"campaignId": 532743},
	})
	require.Nil(t, rpcErr)
	require.NotNil(t, body)

	request, requestErr := (&Router{}).newToolHTTPRequest(context.Background(), "POST", "http://localhost/upload", body)
	require.Nil(t, requestErr)
	require.True(t, strings.HasPrefix(request.Header.Get("Content-Type"), "multipart/form-data; boundary="))
	require.NoError(t, request.ParseMultipartForm(1<<20))
	assert.Equal(t, "campaign", request.FormValue("uploadAction"))
	assert.JSONEq(t, `{"campaignId":532743}`, request.FormValue("data"))

	file, header, err := request.FormFile("file")
	require.NoError(t, err)
	defer file.Close()
	content, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, "campaign.xlsx", header.Filename)
	assert.Equal(t, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", header.Header.Get("Content-Type"))
	assert.Equal(t, "campaign workbook", string(content))
}

func TestMCPMultipartRouteKeepsJSONModeWithoutBlob(t *testing.T) {
	_, parameters := mcpMultipartComponent()
	body, rpcErr := buildMCPMultipartBody(parameters, map[string]interface{}{
		"Data": map[string]interface{}{"campaignId": 532743},
	})
	require.Nil(t, rpcErr)
	assert.Nil(t, body, "JSON-only calls must retain the ordinary application/json path")
}
