package tool

// These scenarios preserve gateway/mcp_multipart_test.go at 69de0137b444.
// Exercise the native route compiler and request providers, without a bridge mock.
import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime/multipart"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

type originalBinaryJSON struct {
	ID int `json:"id"`
}
type originalMultipartInput struct {
	Files  []*multipart.FileHeader `mcp:"name=Files"`
	Action string                  `mcp:"name=Action"`
	Data   originalBinaryJSON      `mcp:"name=Data"`
}
type originalHybridBody struct {
	Data []string `json:"data,omitempty"`
	Raw  []byte   `json:"-"`
}

func (b *originalHybridBody) UnmarshalXLS(_ context.Context, data []byte) error {
	b.Raw = append([]byte(nil), data...)
	return nil
}

type originalHybridInput struct {
	Body *originalHybridBody `mcp:"name=Body"`
}

func originalBinaryPlan(t *testing.T, hybrid bool) *Plan {
	t.Helper()
	typ := reflect.TypeOf(originalMultipartInput{})
	bindings := []bindly.BindingSpec{
		{Path: "Files", Location: bindstate.Location{Kind: "form", In: "file"}},
		{Path: "Action", Location: bindstate.Location{Kind: "form", In: "uploadAction"}},
		{Path: "Data", Location: bindstate.Location{Kind: "body", In: "data"}},
	}
	if hybrid {
		typ = reflect.TypeOf(originalHybridInput{})
		bindings = []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body", In: "body"}}}
	}
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/files", Name: "upload"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "files.upload"}, Contract: testRouteContract(t, typ, bindings)})
	require.NoError(t, err)
	return plan
}
func originalBlob(data string) map[string]interface{} {
	return map[string]interface{}{"data": base64.StdEncoding.EncodeToString([]byte(data)), "filename": "sample.xlsx", "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
}
func originalSchema(t *testing.T, plan *Plan) map[string]interface{} {
	t.Helper()
	data, err := json.Marshal(plan.Metadata().InputSchema)
	require.NoError(t, err)
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &result))
	if result["required"] == nil {
		result["required"] = []interface{}{}
	}
	return result
}
func TestOriginalMCPMultipartToolSchemaOffersBlobAlongsideJSON(t *testing.T) {
	schema := originalSchema(t, originalBinaryPlan(t, false))
	properties := schema["properties"].(map[string]interface{})
	require.Contains(t, properties, "Files")
	files := properties["Files"].(map[string]interface{})
	require.Equal(t, "array", files["type"])
	item := files["items"].(map[string]interface{})
	require.Equal(t, []interface{}{"data"}, item["required"])
	fields := item["properties"].(map[string]interface{})
	require.Contains(t, fields, "data")
	require.Contains(t, fields, "filename")
	require.Contains(t, fields, "mimeType")
	require.Contains(t, properties, "Data")
	require.Contains(t, properties, "Payload")
	require.Equal(t, "object", properties["Payload"].(map[string]interface{})["type"])
	require.NotContains(t, schema["required"], "Data")
	require.NotContains(t, schema["required"], "Payload")
}
func TestOriginalMCPMultipartBodyCarriesBlobFormAndJSON(t *testing.T) {
	scope, err := originalBinaryPlan(t, false).Scope(map[string]interface{}{
		"Files": []interface{}{originalBlob("sample workbook")}, "Action": "import", "Data": map[string]interface{}{"id": 7},
		"Payload": map[string]interface{}{"ownerId": 9, "requiredWidth": 300, "channels": []interface{}{"DISPLAY", "VIDEO"}, "options": map[string]interface{}{"secure": true}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, scope.Close()) })
	value, found, err := scope.Form().Locate(nil).Value(context.Background(), reflect.TypeOf([]*multipart.FileHeader{}), "file")
	require.NoError(t, err)
	require.True(t, found)
	headers, ok := value.([]*multipart.FileHeader)
	require.True(t, ok)
	require.Len(t, headers, 1)
	require.Equal(t, "sample.xlsx", headers[0].Filename)
	require.Equal(t, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers[0].Header.Get("Content-Type"))
	file, err := headers[0].Open()
	require.NoError(t, err)
	defer file.Close()
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, "sample workbook", string(data))
	for name, want := range map[string]string{"uploadAction": "import", "ownerId": "9", "requiredWidth": "300", "data": `{"id":7}`, "options": `{"secure":true}`} {
		value, found, err := scope.Form().Locate(nil).Value(context.Background(), reflect.TypeOf(""), name)
		require.NoError(t, err)
		require.True(t, found, name)
		if name == "data" || name == "options" {
			require.JSONEq(t, want, value.(string))
		} else {
			require.Equal(t, want, value, name)
		}
	}
	value, found, err = scope.Form().Locate(nil).Value(context.Background(), reflect.TypeOf([]string{}), "channels")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"DISPLAY", "VIDEO"}, value)
}
func TestOriginalMCPMultipartPayloadCannotOverrideDeclaredFormFields(t *testing.T) {
	_, err := originalBinaryPlan(t, false).Scope(map[string]interface{}{"Files": []interface{}{originalBlob("x")}, "Payload": map[string]interface{}{"uploadAction": "override"}})
	require.ErrorContains(t, err, "conflicts with a declared form parameter")
}
func TestOriginalMCPMultipartPayloadRequiresFiles(t *testing.T) {
	_, err := originalBinaryPlan(t, false).Scope(map[string]interface{}{"Payload": map[string]interface{}{"ownerId": 9}})
	require.ErrorContains(t, err, "requires Files")
}
func TestOriginalMCPMultipartRouteKeepsJSONModeWithoutBlob(t *testing.T) {
	scope, err := originalBinaryPlan(t, false).Scope(map[string]interface{}{"Data": map[string]interface{}{"id": 7}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, scope.Close()) })
	value, found, err := scope.Body().Locate(nil).Value(context.Background(), reflect.TypeOf(originalBinaryJSON{}), "data")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, originalBinaryJSON{ID: 7}, value)
}
func TestOriginalMCPHybridBodySchemaOffersEitherJSONOrRawBlob(t *testing.T) {
	schema := originalSchema(t, originalBinaryPlan(t, true))
	properties := schema["properties"].(map[string]interface{})
	require.Contains(t, properties, "Body")
	require.Contains(t, properties, "Blob")
	require.Equal(t, []interface{}{"data"}, properties["Blob"].(map[string]interface{})["required"])
	require.NotContains(t, schema["required"], "Body")
	require.NotContains(t, schema["required"], "Blob")
}
func TestOriginalMCPHybridBodyRawBlobUsesBinaryContentType(t *testing.T) {
	scope, err := originalBinaryPlan(t, true).Scope(map[string]interface{}{"Blob": originalBlob("xlsx bytes")})
	require.NoError(t, err)
	// The existing XLS decoder only receives these bytes when the request body
	// provider selects the original descriptor's XLS MIME, rather than JSON.
	value, found, err := scope.Body().Locate(nil).Value(context.Background(), reflect.TypeOf(&originalHybridBody{}), "body")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("xlsx bytes"), value.(*originalHybridBody).Raw)
}
