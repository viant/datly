package openapi_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xdocs "github.com/viant/xdatly/docs"
	"github.com/viant/xdatly/response"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/fstest"
)

const responseYAML = `Paths:
  /download: Download bytes
Responses:
  /download:
    '201':
      description: Authored bytes
      headers:
        X-Result:
          schema: {type: string}
          example: ready
      content:
        application/octet-stream:
          schema: {type: string, format: binary}
          example: opaque
`

type responseTrap struct{ *response.Buffered }

func (*responseTrap) MarshalJSON() ([]byte, error) {
	panic("transport response must not be serialized")
}
func TestAuthoredGenericResponseDocumentationAndDirectBytes(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[*response.Buffered](), reflect.TypeFor[response.Response](), reflect.TypeFor[*responseTrap]()} {
		t.Run(typ.String(), func(t *testing.T) {
			ctx := context.Background()
			store := resource.New()
			require.NoError(t, store.Register("p", fstest.MapFS{"docs.yaml": {Data: []byte(responseYAML)}}))
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Download"}, Documentation: xdocs.Source{DocURL: "p:docs.yaml"}, Routes: []*spec.Route{{Method: "GET", Path: "/download", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "download"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Resources: store})
			require.NoError(t, err)
			var calls atomic.Int32
			payload := []byte{0, 255, 1, 254, 2}
			var invokeError error
			compression := ""
			noBody := false
			entry, err := artifact.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(context.Context, handler.Invocation) (any, error) {
				calls.Add(1)
				buffer := response.NewBuffered(response.WithBytes(payload), response.WithStatusCode(201), response.WithHeader("X-Result", "ready"), response.WithHeader("Content-Type", "application/octet-stream"), response.WithCompressionType(compression))
				if noBody {
					buffer = &response.Buffered{}
					buffer.SetStatusCode(201)
				}
				if typ == reflect.TypeFor[*responseTrap]() {
					return &responseTrap{buffer}, invokeError
				}
				return buffer, invokeError
			})})
			require.NoError(t, err)
			doc, err := (openapi.Generator{}).Generate(ctx, documentRequest(entry))
			require.NoError(t, err)
			require.Equal(t, int32(0), calls.Load())
			require.Equal(t, "binary", doc.Paths["/download"].Get.Responses["201"].Content["application/octet-stream"].Schema.Format)
			require.Equal(t, "ready", doc.Paths["/download"].Get.Responses["201"].Headers["X-Result"].Example)
			rt, err := runtime.NewRuntime([]*registry.RegisteredComponent{entry})
			require.NoError(t, err)
			defer rt.Shutdown(ctx)
			service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: rt, Resources: store})
			require.NoError(t, err)
			tool, ok := service.Catalog().Tool("download")
			require.True(t, ok)
			metadata := tool.Metadata()
			require.Contains(t, metadata.Meta, "datly/httpResponses")
			require.Nil(t, metadata.OutputSchema)
			metadata.Meta["datly/httpResponses"] = "changed"
			require.NotEqual(t, metadata.Meta, tool.Metadata().Meta)
			require.Equal(t, int32(0), calls.Load())
			httpHandler := gateway.NewHandler(rt, nil, "")
			for _, kind := range []string{"binary", "empty", "nil-body", "compressed", "error"} {
				t.Run(kind, func(t *testing.T) {
					noBody = false
					switch kind {
					case "nil-body":
						payload = nil
						noBody = true
					case "empty":
						payload = nil
					case "compressed":
						var buffer bytes.Buffer
						writer := gzip.NewWriter(&buffer)
						_, err := writer.Write([]byte("already compressed"))
						require.NoError(t, err)
						require.NoError(t, writer.Close())
						payload = buffer.Bytes()
						compression = "gzip"
					case "error":
						invokeError = errors.New("private detail")
					}
					recorder := httptest.NewRecorder()
					httpHandler.ServeHTTP(recorder, httptest.NewRequest("GET", "/download", nil))
					if kind == "error" {
						require.GreaterOrEqual(t, recorder.Code, 400)
						require.NotContains(t, recorder.Body.String(), "private detail")
						return
					}
					require.Equal(t, 201, recorder.Code)
					require.Equal(t, payload, recorder.Body.Bytes())
					if !noBody {
						require.Equal(t, "ready", recorder.Header().Get("X-Result"))
					}
					if compression != "" {
						require.Equal(t, "gzip", recorder.Header().Get("Content-Encoding"))
					}
				})
			}
		})
	}
}
