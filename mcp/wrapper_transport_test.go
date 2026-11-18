package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xeipuuv/gojsonschema"
)

type wrapperTransportInput struct {
	Body []int `parameter:"Body,kind=body" mcp:"name=creativeIds" jsonx:"wrapper=ids"`
}

type wrapperTransportOutput struct {
	IDs []int `json:"ids"`
}

func TestWrapperNativeMCPTransport(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(fmt.Sprintf("strict=%v", strict), func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "wrapped_ids"}, Routes: []*spec.Route{{Method: "POST", Path: "/wrapped-ids", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "wrapped_ids", StrictArguments: &strict}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[wrapperTransportInput](), OutputType: reflect.TypeFor[wrapperTransportOutput]()})
			require.NoError(t, err)
			calls := 0
			var actual []int
			entry, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *wrapperTransportInput) (*wrapperTransportOutput, error) {
				calls++
				actual = in.Body
				return &wrapperTransportOutput{IDs: in.Body}, nil
			})})
			require.NoError(t, err)
			runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
			service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: runtime})
			require.NoError(t, err)
			server, err := mcpserver.New(mcpserver.Config{Service: service, Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStreamable}})
			require.NoError(t, err)
			httpServer, err := server.HTTP()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
			rpc := func(method, args string) json.RawMessage {
				params := fmt.Sprintf(`{"_meta":{"io.modelcontextprotocol/protocolVersion":%q,"io.modelcontextprotocol/clientCapabilities":{}}}`, schema.LatestProtocolVersion)
				if method == schema.MethodToolsCall {
					params = fmt.Sprintf(`{"name":"wrapped_ids","arguments":%s,"_meta":{"io.modelcontextprotocol/protocolVersion":%q,"io.modelcontextprotocol/clientCapabilities":{}}}`, args, schema.LatestProtocolVersion)
				}
				req := httptest.NewRequest("POST", "/mcp", strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, params)))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Accept", "application/json, text/event-stream")
				req.Header.Set(schema.HeaderProtocolVersion, schema.LatestProtocolVersion)
				req.Header.Set(schema.HeaderMethod, method)
				if method == schema.MethodToolsCall {
					req.Header.Set(schema.HeaderName, "wrapped_ids")
				}
				rec := httptest.NewRecorder()
				httpServer.Handler.ServeHTTP(rec, req)
				require.Equal(t, 200, rec.Code, rec.Body.String())
				if strings.HasPrefix(rec.Header().Get("Content-Type"), "text/event-stream") {
					var lines []string
					for _, line := range strings.Split(rec.Body.String(), "\n") {
						if strings.HasPrefix(line, "data:") {
							lines = append(lines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
						}
					}
					return json.RawMessage(strings.Join(lines, "\n"))
				}
				return append(json.RawMessage(nil), rec.Body.Bytes()...)
			}
			var listed struct {
				Result struct {
					Tools []struct{ InputSchema, OutputSchema json.RawMessage }
				}
			}
			require.NoError(t, json.Unmarshal(rpc(schema.MethodToolsList, ""), &listed))
			require.Len(t, listed.Result.Tools, 1)
			require.Zero(t, calls)
			validator, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(listed.Result.Tools[0].InputSchema))
			require.NoError(t, err)
			outputValidator, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(listed.Result.Tools[0].OutputSchema))
			require.NoError(t, err)
			for _, tc := range []struct {
				raw   string
				valid bool
			}{{`{"ids":[1]}`, true}, {`{"ids":{"ids":[1]}}`, false}} {
				result, err := outputValidator.Validate(gojsonschema.NewStringLoader(tc.raw))
				require.NoError(t, err)
				require.Equal(t, tc.valid, result.Valid(), "output representation changed: %v", result.Errors())
			}
			for _, tc := range []struct {
				args  string
				want  []int
				valid bool
			}{
				{`{}`, nil, true}, {`{"creativeIds":null}`, nil, true}, {`{"creativeIds":[]}`, []int{}, true},
				{`{"creativeIds":[1,2]}`, []int{1, 2}, true}, {`{"creativeIds":{"ids":[3]}}`, []int{3}, true},
				{`{"creativeIds":{"ids":[]}}`, []int{}, true},
				{`{"creativeIds":{}}`, nil, false}, {`{"creativeIds":{"ids":null}}`, nil, false},
				{`{"creativeIds":{"ids":[1],"other":2}}`, nil, false},
				{`{"creativeIds":{"ids":[1.5]}}`, nil, false},
			} {
				t.Run(tc.args, func(t *testing.T) {
					validation, err := validator.Validate(gojsonschema.NewStringLoader(tc.args))
					require.NoError(t, err)
					require.Equal(t, tc.valid, validation.Valid(), "%v", validation.Errors())
					before := calls
					wire := rpc(schema.MethodToolsCall, tc.args)
					var response struct {
						Error  any
						Result struct {
							IsError           bool
							StructuredContent json.RawMessage
						}
					}
					require.NoError(t, json.Unmarshal(wire, &response))
					require.Equal(t, !tc.valid, response.Error != nil || response.Result.IsError, string(wire))
					if !tc.valid {
						require.Equal(t, before, calls)
						if !strict {
							messages := map[string]string{
								`{"creativeIds":{}}`:                    "ids is required",
								`{"creativeIds":{"ids":null}}`:          "ids is required",
								`{"creativeIds":{"ids":[1],"other":2}}`: `json: unknown field "other"`,
								`{"creativeIds":{"ids":[1.5]}}`:         "Bad Request",
							}
							var failure struct{ Message string }
							require.NoError(t, json.Unmarshal(response.Result.StructuredContent, &failure), string(wire))
							require.Equal(t, messages[tc.args], failure.Message, string(wire))
						}
						return
					}
					require.Equal(t, before+1, calls)
					require.Equal(t, tc.want, actual)
					var output wrapperTransportOutput
					require.NoError(t, json.Unmarshal(response.Result.StructuredContent, &output))
					require.Equal(t, tc.want, output.IDs)
				})
			}
		})
	}
}
