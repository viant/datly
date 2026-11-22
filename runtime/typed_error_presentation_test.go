package runtime_test

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

type errorPresentationInput struct {
	Mode string `parameter:"Mode,kind=query,in=mode"`
}
type errorPresentationLegacy struct {
	Status  string   `json:"status"`
	Message string   `json:"message"`
	Data    []string `json:"data"`
}
type errorPresentationRow struct {
	Attribute json.RawMessage
	Secret    string `json:"-"`
}
type errorPresentationJSON struct {
	Data       []errorPresentationRow
	Violations []string `json:"violations,omitempty"`
}
type errorPresentationSuccess struct {
	Value string `json:"value"`
}

func TestTypedErrorPresentation(t *testing.T) {
	const path = "/typed-error-presentation"
	const toolName = "typed_error_presentation"
	const media = "text/plain; charset=utf-8"
	const message = "escaped <tag> & \"quote\"\n café \u2028 \x00 \x07 \x1f"
	privateCause := errors.New("private error sentinel")
	legacy := errorPresentationLegacy{Status: "error", Message: message, Data: []string{}}
	legacyBytes, err := json.Marshal(legacy)
	require.NoError(t, err)
	component := &spec.Component{
		ErrorOutputs: []spec.ErrorOutput{{Type: reflect.TypeFor[errorPresentationLegacy](), Presentation: spec.ErrorOutputLegacyText}, {Type: reflect.TypeFor[errorPresentationJSON]()}},
		Key:          spec.Key{Kind: spec.KindComponent, Name: "typedErrorPresentation"},
		Routes:       []*spec.Route{{Method: "GET", Path: path, MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: toolName}}}},
		Settings:     &spec.Settings{CaseFormat: "lc", ResponseCompression: &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 1}, Output: &spec.OutputSettings{Headers: map[string]string{"X-Success": "yes"}}},
	}
	handler := custom.NewFunc(func(_ context.Context, input *errorPresentationInput) (*errorPresentationSuccess, error) {
		switch input.Mode {
		case "legacy":
			return nil, &response.Error{Code: 401, Cause: errors.Join(privateCause, errors.New("secondary cause")), Payload: &legacy}
		case "json":
			return nil, &response.Error{Code: 400, Cause: privateCause, Payload: errorPresentationJSON{Data: []errorPresentationRow{{Attribute: json.RawMessage(`{"kept":true}`), Secret: "secret"}}}}
		case "failure":
			return nil, &response.Error{Code: 400, Cause: privateCause, Payload: errorPresentationJSON{Data: []errorPresentationRow{{Attribute: json.RawMessage(`{`)}}}}
		case "default":
			return nil, &response.Error{Code: 400, Cause: privateCause, Payload: &errorPresentationSuccess{Value: "ordinary typed"}}
		case "other":
			return nil, &response.Error{Code: 400, Cause: privateCause, Payload: map[string]string{"generic": "fallback"}}
		case "null":
			return nil, &response.Error{Code: 400, Cause: privateCause}
		case "typed-null":
			return nil, &response.Error{Code: 400, Cause: privateCause, Payload: (*errorPresentationLegacy)(nil)}
		case "raw":
			buffer := response.NewBuffered(response.WithStatusCode(422))
			buffer.Headers().Set("Content-Type", "application/octet-stream")
			_, _ = buffer.Write([]byte("original transport"))
			return nil, &response.Error{Code: 400, Cause: privateCause, Payload: buffer}
		}
		return &errorPresentationSuccess{Value: "successful output"}, nil
	})
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[errorPresentationInput](), OutputType: reflect.TypeFor[errorPresentationSuccess](), Handler: handler, HandlerOwnedOutput: true})
	require.NoError(t, err)
	entry, err := artifact.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	h := gateway.NewHandler(rt, nil, "typed error presentation")
	read := func(mode string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path+"?mode="+mode, nil))
		return rec
	}
	inflate := func(rec *httptest.ResponseRecorder) string {
		t.Helper()
		require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
		reader, err := gzip.NewReader(rec.Body)
		require.NoError(t, err)
		defer reader.Close()
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		return string(data)
	}
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: rt})
	require.NoError(t, err)
	tool, found := service.Registry().ToolRegistry.Get(toolName)
	require.True(t, found)
	call := func(mode string) *schema.CallToolResult {
		t.Helper()
		result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall,
			Params: schema.CallToolRequestParams{Name: toolName, Arguments: map[string]any{"mode": mode}}})
		require.Nil(t, rpcErr)
		return result
	}
	t.Run("legacy HTTP", func(t *testing.T) {
		rec := read("legacy")
		require.Equal(t, 401, rec.Code)
		require.Equal(t, media, rec.Header().Get("Content-Type"))
		require.Equal(t, strconv.Itoa(len(legacyBytes)), rec.Header().Get("Content-Length"))
		require.Empty(t, rec.Header().Get("Content-Encoding"))
		require.Empty(t, rec.Header().Get("X-Success"))
		require.Equal(t, string(legacyBytes), rec.Body.String())
	})
	t.Run("legacy MCP", func(t *testing.T) {
		result := call("legacy")
		require.NotNil(t, result.IsError)
		require.True(t, *result.IsError)
		status, ok := schema.ToolErrorHTTPStatus(result)
		require.True(t, ok)
		require.Equal(t, 401, status)
		require.Nil(t, result.StructuredContent)
		require.Len(t, result.Content, 1)
		blob, ok := result.Content[0].(schema.EmbeddedResource)
		require.True(t, ok)
		require.Equal(t, media, *blob.Resource.MimeType)
		data, err := base64.StdEncoding.DecodeString(blob.Resource.Blob)
		require.NoError(t, err)
		require.Equal(t, legacyBytes, data)
	})
	t.Run("alternate JSON", func(t *testing.T) {
		want := `{"data":[{"attribute":{"kept":true}}]}`
		rec := read("json")
		require.Equal(t, 400, rec.Code)
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.Empty(t, rec.Header().Get("X-Success"))
		require.JSONEq(t, want, inflate(rec))
		result := call("json")
		require.NotNil(t, result.IsError)
		require.True(t, *result.IsError)
		require.JSONEq(t, want, result.Content[0].(schema.TextContent).Text)
		structured, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		require.JSONEq(t, want, string(structured))
	})
	t.Run("ordinary output", func(t *testing.T) {
		rec := read("default")
		require.Equal(t, 400, rec.Code)
		require.JSONEq(t, `{"value":"ordinary typed"}`, inflate(rec))
		require.JSONEq(t, `{"value":"ordinary typed"}`, call("default").Content[0].(schema.TextContent).Text)
		rec = read("other")
		require.Equal(t, 400, rec.Code)
		require.JSONEq(t, `{"generic":"fallback"}`, inflate(rec))
		require.JSONEq(t, `{"generic":"fallback"}`, call("other").Content[0].(schema.TextContent).Text)
		rec = read("success")
		require.Equal(t, 200, rec.Code)
		require.Equal(t, "yes", rec.Header().Get("X-Success"))
		require.JSONEq(t, `{"value":"successful output"}`, inflate(rec))
	})
	t.Run("explicit null and raw transport", func(t *testing.T) {
		rec := read("null")
		require.Equal(t, 400, rec.Code)
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.Equal(t, "null", inflate(rec))
		require.Equal(t, "null", call("null").Content[0].(schema.TextContent).Text)
		rec = read("typed-null")
		require.Equal(t, 400, rec.Code)
		require.Equal(t, media, rec.Header().Get("Content-Type"))
		require.Equal(t, "null", rec.Body.String())
		require.Equal(t, "4", rec.Header().Get("Content-Length"))
		typed := call("typed-null")
		require.Nil(t, typed.StructuredContent)
		require.Equal(t, "bnVsbA==", typed.Content[0].(schema.EmbeddedResource).Resource.Blob)

		rec = read("raw")
		require.Equal(t, 400, rec.Code)
		require.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
		require.Equal(t, "original transport", rec.Body.String())
		raw := call("raw")
		require.Nil(t, raw.StructuredContent)
		resource := raw.Content[0].(schema.EmbeddedResource).Resource
		require.Equal(t, "application/octet-stream", *resource.MimeType)
		require.Equal(t, base64.StdEncoding.EncodeToString(rec.Body.Bytes()), resource.Blob)
	})
	t.Run("encoding failure", func(t *testing.T) {
		rec := read("failure")
		require.Equal(t, 500, rec.Code)
		require.NotContains(t, rec.Body.String(), privateCause.Error())
		result := call("failure")
		require.JSONEq(t, `{"status":500,"error":true,"message":"Internal Server Error"}`, result.Content[0].(schema.TextContent).Text)
	})
}
