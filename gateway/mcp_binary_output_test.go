package gateway

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/router/proxy"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xuri/excelize/v2"
)

const binarySpreadsheetMIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

func binaryOutputFixture(t *testing.T) []byte {
	t.Helper()
	book := excelize.NewFile()
	defer book.Close()
	require.NoError(t, book.SetCellValue("Sheet1", "A1", "Campaign"))
	_, err := book.NewSheet("Ads")
	require.NoError(t, err)
	require.NoError(t, book.SetCellValue("Ads", "A1", "Creative"))
	buffer, err := book.WriteToBuffer()
	require.NoError(t, err)
	return buffer.Bytes()
}

func binaryResponse(body []byte, mediaType, disposition, encoding string, status int) *proxy.Writer {
	writer := proxy.NewWriter()
	writer.Code = status
	writer.HeaderMap.Set("Content-Type", mediaType)
	writer.HeaderMap.Set("Content-Disposition", disposition)
	writer.HeaderMap.Set("Content-Encoding", encoding)
	writer.Body.Write(body)
	return writer
}

func gzipBinaryOutput(t *testing.T, data []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return output.Bytes()
}

func assertBinaryOutput(t *testing.T, result *schema.CallToolResult, expected []byte, mediaType string) {
	t.Helper()
	require.Nil(t, result.IsError)
	require.Nil(t, result.StructuredContent)
	require.Len(t, result.Content, 1)
	wire, err := json.Marshal(result.Content[0])
	require.NoError(t, err)
	var envelope struct {
		Type     string `json:"type"`
		Resource struct {
			Uri      string  `json:"uri"`
			Blob     string  `json:"blob"`
			MimeType *string `json:"mimeType"`
			Text     string  `json:"text"`
		} `json:"resource"`
	}
	require.NoError(t, json.Unmarshal(wire, &envelope))
	require.Equal(t, "resource", envelope.Type)
	var standard schema.EmbeddedResource
	require.NoError(t, json.Unmarshal(wire, &standard))
	require.Equal(t, envelope.Resource.Blob, standard.Resource.Blob)
	require.Empty(t, standard.Meta)
	require.Empty(t, standard.Resource.Meta)
	require.Empty(t, envelope.Resource.Text)
	require.NotNil(t, envelope.Resource.MimeType)
	require.Equal(t, mediaType, *envelope.Resource.MimeType)
	decoded, err := base64.StdEncoding.DecodeString(envelope.Resource.Blob)
	require.NoError(t, err)
	require.True(t, bytes.Equal(expected, decoded), "binary output changed")
	uri, err := url.Parse(envelope.Resource.Uri)
	require.NoError(t, err)
	require.Equal(t, "urn", uri.Scheme)
	require.Empty(t, uri.RawQuery)
	require.Empty(t, uri.User)
	require.NotContains(t, uri.String(), "private")
	require.NotContains(t, uri.String(), "secret")
}

func TestMCPBinaryToolOutput(t *testing.T) {
	spreadsheet := binaryOutputFixture(t)
	for _, test := range []struct {
		name                             string
		data                             []byte
		mediaType, disposition, encoding string
	}{
		{"invalidUTF8", []byte{0, 255, 254, 128}, "application/octet-stream", "", ""},
		{"xlsx", spreadsheet, binarySpreadsheetMIME, `attachment; filename="campaign.xlsx"`, ""},
		{"gzipXlsx", spreadsheet, binarySpreadsheetMIME, "", "gzip"},
		{"textAttachment", []byte("name,value\nfoo,42"), "text/csv", `attachment; filename="report.csv"`, ""},
		{"missingMIME", []byte{0, 255, 254}, "", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := test.data
			if test.encoding == "gzip" {
				body = gzipBinaryOutput(t, body)
			}
			result := (&Router{}).buildToolCallResult(binaryResponse(body, test.mediaType, test.disposition, test.encoding, 200), "http://user:secret@localhost/private/file?token=secret", "GET")
			mediaType := test.mediaType
			if mediaType == "" {
				mediaType = "application/octet-stream"
			}
			assertBinaryOutput(t, result, test.data, mediaType)
			if test.name == "xlsx" || test.name == "gzipXlsx" {
				book, err := excelize.OpenReader(bytes.NewReader(test.data))
				require.NoError(t, err)
				defer book.Close()
				require.Equal(t, []string{"Sheet1", "Ads"}, book.GetSheetList())
			}
		})
	}
}

func TestMCPBinaryToolErrorsDoNotEchoBytes(t *testing.T) {
	for _, test := range []struct {
		name, mediaType, encoding string
		status                    int
		data                      []byte
	}{
		{"binaryHTTPError", "application/octet-stream", "", 500, []byte("private binary payload\x00\xff")},
		{"badGzip", binarySpreadsheetMIME, "gzip", 200, []byte("private binary payload\x00\xff")},
		{"truncatedGzip", binarySpreadsheetMIME, "gzip", 200, gzipBinaryOutput(t, []byte("private binary payload"))[:15]},
		{"unsupportedEncoding", binarySpreadsheetMIME, "br", 200, []byte("private binary payload")},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := (&Router{}).buildToolCallResult(binaryResponse(test.data, test.mediaType, "", test.encoding, test.status), "http://localhost/test", "GET")
			require.NotNil(t, result.IsError)
			require.True(t, *result.IsError)
			wire, err := json.Marshal(result)
			require.NoError(t, err)
			require.NotContains(t, string(wire), "private binary payload")
			require.NotContains(t, string(wire), base64.StdEncoding.EncodeToString(test.data))
		})
	}
}

func TestMCPOrdinaryToolOutputCompatible(t *testing.T) {
	for _, test := range []struct {
		name, mediaType, encoding string
		status                    int
		body                      string
	}{
		{"json", "application/json", "", 200, `{"data":[1]}`},
		{"text", "text/plain; charset=utf-8", "", 200, "ordinary text"},
		{"jsonError", "application/json", "", 400, `{"message":"invalid request"}`},
		{"gzipJSON", "application/json", "gzip", 200, `{"data":[1]}`},
		{"defaultJSON", "", "", 200, `{"data":[1]}`},
		{"jsonSuffix", "application/problem+json", "", 400, `{"message":"invalid request"}`},
		{"textUnicode", "text/plain", "", 200, "広告 – Café"},
		{"gzipJSONError", "application/json", "gzip", 400, `{"message":"invalid request"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(test.body)
			if test.encoding == "gzip" {
				body = gzipBinaryOutput(t, body)
			}
			result := (&Router{}).buildToolCallResult(binaryResponse(body, test.mediaType, "", test.encoding, test.status), "http://localhost/test", "GET")
			require.Equal(t, schema.TextContent{Type: "text", Text: test.body}, result.Content[0])
			if test.status >= 400 {
				require.True(t, *result.IsError)
				require.Equal(t, test.body, result.StructuredContent.(map[string]interface{})["message"])
			}
		})
	}
}

func TestMCPBinaryResourceRead(t *testing.T) {
	for _, test := range []struct {
		name, mediaType, encoding string
		status                    int
		data                      []byte
	}{
		{"xlsx", binarySpreadsheetMIME, "", 200, binaryOutputFixture(t)},
		{"gzip", "application/octet-stream", "gzip", 200, []byte{0, 255, 128}},
		{"json", "application/json", "", 200, []byte(`{"ok":true}`)},
		{"binaryError", binarySpreadsheetMIME, "", 500, []byte{0, 255, 128}},
		{"badGzip", binarySpreadsheetMIME, "gzip", 200, []byte{0, 255, 128}},
	} {
		t.Run(test.name, func(t *testing.T) {
			component := &repository.Component{View: &view.View{}}
			component.View.SetResource(&view.Resource{})
			provider := repository.NewProvider(contract.Path{}, &version.Control{}, func(context.Context, ...repository.Option) (*repository.Component, error) { return component, nil })
			body := test.data
			if test.name == "gzip" {
				body = gzipBinaryOutput(t, body)
			}
			route := &Route{Handler: func(_ context.Context, writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", test.mediaType)
				writer.Header().Set("Content-Encoding", test.encoding)
				writer.WriteHeader(test.status)
				writer.Write(body)
			}}
			fallback := "application/json"
			results, rpcErr := (&Router{}).handleMcpRead(context.Background(), &schema.ReadResourceRequestParams{Uri: "datly://localhost/private/file?token=secret"}, &schema.ResourceTemplate{MimeType: &fallback}, route, provider)
			if test.name == "binaryError" || test.name == "badGzip" {
				require.NotNil(t, rpcErr)
				require.Empty(t, results)
				return
			}
			require.Nil(t, rpcErr)
			require.Len(t, results, 1)
			require.Equal(t, test.mediaType, *results[0].MimeType)
			if test.name == "json" {
				require.Equal(t, string(test.data), results[0].Text)
				require.Empty(t, results[0].Blob)
				return
			}
			require.Empty(t, results[0].Text)
			wire, err := json.Marshal(results[0])
			require.NoError(t, err)
			var standard schema.BlobResourceContents
			require.NoError(t, json.Unmarshal(wire, &standard))
			require.Equal(t, results[0].Blob, standard.Blob)
			decoded, err := base64.StdEncoding.DecodeString(results[0].Blob)
			require.NoError(t, err)
			require.True(t, bytes.Equal(test.data, decoded), "binary resource changed")
			require.NotContains(t, results[0].Uri, "private")
			require.NotContains(t, results[0].Uri, "secret")
		})
	}
}
