package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/bindly"
	xcontract "github.com/viant/xdatly/contract"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xeipuuv/gojsonschema"
)

type representedJSONXRow struct {
	Q *string `json:"q,omitempty" jsonx:"numberToString"`
}
type representedJSONXBody struct {
	Secret string                                             `json:"secret,omitempty"`
	Q      *string                                            `json:"q,omitempty" jsonx:"numberToString"`
	Flag   *int                                               `json:"flag,omitempty" jsonx:"boolToInt,null=omit"`
	Bill   *string                                            `json:"bill,omitempty" jsonx:"boolIntToYN,null=omit"`
	Plain  *int                                               `json:"plain,omitempty"`
	Rows   []representedJSONXRow                              `json:"rows,omitempty"`
	Has    *struct{ Q, Flag, Bill, Plain, Rows, Secret bool } `json:"-" setMarker:"true"`
}
type representedJSONXFlatInput struct {
	Body *representedJSONXBody `parameter:",kind=body" anonymous:"true"`
}
type representedJSONXNamedInput struct {
	Body *representedJSONXBody `parameter:",kind=body,in=payload"`
}
type representedJSONXOutput struct {
	OK bool `json:"ok"`
}

var representedJSONXPreparations atomic.Int32

func representedJSONXPolicy(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var input struct {
		Enabled string `parameter:",kind=header,in=X-Representation"`
	}
	if err := binder.Bind(ctx, &input); err != nil {
		return nil, err
	}
	if input.Enabled != "true" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "hide-secret", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		representedJSONXPreparations.Add(1)
		if parent == reflect.TypeFor[representedJSONXBody]() && field.Name == "Secret" {
			return field.Clone().Hide(), nil
		}
		return nil, nil
	}}, nil
}
func (*representedJSONXFlatInput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	return representedJSONXPolicy(ctx, binder)
}
func (*representedJSONXNamedInput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	return representedJSONXPolicy(ctx, binder)
}

// This uses the actual protocol JSON decoder, so numeric arguments traverse the
// same HTTP -> MCP arguments -> native body binding boundary as a remote client.
func TestRepresentedJSONXNativeHTTPAndREST(t *testing.T) {
	for _, represented := range []bool{false, true} {
		for _, strict := range []bool{false, true} {
			for _, named := range []bool{false, true} {
				t.Run(fmt.Sprintf("represented=%v/strict=%v/named=%v", represented, strict, named), func(t *testing.T) {
					inputType := reflect.TypeFor[representedJSONXFlatInput]()
					if named {
						inputType = reflect.TypeFor[representedJSONXNamedInput]()
					}
					component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "strict_jsonx"}, Routes: []*spec.Route{{Method: "POST", Path: "/strict-jsonx", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "strict_jsonx", StrictArguments: &strict}}}}}
					artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: reflect.TypeFor[representedJSONXOutput]()})
					require.NoError(t, err)
					calls := 0
					var bound *representedJSONXBody
					record := func(body *representedJSONXBody) (*representedJSONXOutput, error) {
						calls++
						bound = body
						return &representedJSONXOutput{OK: true}, nil
					}
					registered := registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, input *representedJSONXFlatInput) (*representedJSONXOutput, error) {
						return record(input.Body)
					})}
					if named {
						registered.Handler = custom.NewFunc(func(_ context.Context, input *representedJSONXNamedInput) (*representedJSONXOutput, error) {
							return record(input.Body)
						})
					}
					entry, err := artifact.Registration(registered)
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
					rpc := func(method, body string) json.RawMessage {
						params := fmt.Sprintf(`{"_meta":{"io.modelcontextprotocol/protocolVersion":%q,"io.modelcontextprotocol/clientCapabilities":{}}}`, schema.LatestProtocolVersion)
						if method == schema.MethodToolsCall {
							if named {
								body = `{"body":` + body + `}`
							}
							params = fmt.Sprintf(`{"name":"strict_jsonx","arguments":%s,"_meta":{"io.modelcontextprotocol/protocolVersion":%q,"io.modelcontextprotocol/clientCapabilities":{}}}`, body, schema.LatestProtocolVersion)
						}
						request := httptest.NewRequest("POST", "/mcp", strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, params)))
						request.Header.Set("Content-Type", "application/json")
						request.Header.Set("X-Representation", strconv.FormatBool(represented))
						request.Header.Set("Accept", "application/json, text/event-stream")
						request.Header.Set(schema.HeaderProtocolVersion, schema.LatestProtocolVersion)
						request.Header.Set(schema.HeaderMethod, method)
						if method == schema.MethodToolsCall {
							request.Header.Set(schema.HeaderName, "strict_jsonx")
						}
						recorder := httptest.NewRecorder()
						httpServer.Handler.ServeHTTP(recorder, request)
						require.Equal(t, 200, recorder.Code, recorder.Body.String())
						if strings.HasPrefix(recorder.Header().Get("Content-Type"), "text/event-stream") {
							var data []string
							for _, line := range strings.Split(recorder.Body.String(), "\n") {
								if strings.HasPrefix(line, "data:") {
									data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
								}
							}
							return json.RawMessage(strings.Join(data, "\n"))
						}
						return append(json.RawMessage(nil), recorder.Body.Bytes()...)
					}
					var listed struct {
						Result struct {
							Tools []struct{ InputSchema json.RawMessage }
						}
					}
					require.NoError(t, json.Unmarshal(rpc(schema.MethodToolsList, ""), &listed))
					require.Len(t, listed.Result.Tools, 1)
					require.Zero(t, calls)
					validator, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(listed.Result.Tools[0].InputSchema))
					require.NoError(t, err)
					for _, tc := range []struct {
						name, raw, q string
						valid        bool
					}{
						{"numeric", `{"q":50,"flag":true,"bill":"","rows":[{"q":"51"}]}`, "50", true},
						{"string", `{"q":"50","flag":null}`, "50", true},
						{"fraction", `{"q":50.5}`, "50.5", true},
						{"bool rejected", `{"q":true}`, "", false},
						{"plain remains strict", `{"q":"50","plain":true}`, "", false},
						{"nested unknown", `{"q":"50","rows":[{"q":"51","unexpected":true}]}`, "50", !strict},
					} {
						t.Run(tc.name, func(t *testing.T) {
							args := tc.raw
							if named {
								args = `{"body":` + args + `}`
							}
							valid, err := validator.Validate(gojsonschema.NewStringLoader(args))
							require.NoError(t, err)
							if tc.name != "nested unknown" {
								require.Equal(t, tc.valid, valid.Valid(), "discovered schema: %s", listed.Result.Tools[0].InputSchema)
							}
							before := calls
							wire := rpc(schema.MethodToolsCall, tc.raw)
							var result struct {
								Error  any
								Result struct {
									IsError           bool
									StructuredContent struct{ Status int }
								}
							}
							require.NoError(t, json.Unmarshal(wire, &result))
							require.Equal(t, !tc.valid, result.Error != nil || result.Result.IsError, string(wire))
							if !tc.valid {
								require.Equal(t, 400, result.Result.StructuredContent.Status, string(wire))
								require.Equal(t, before, calls)
								return
							}
							require.Equal(t, before+1, calls)
							require.NotNil(t, bound.Q)
							require.Equal(t, tc.q, *bound.Q)
							require.True(t, bound.Has.Q)
							if tc.name == "numeric" {
								require.Equal(t, 1, *bound.Flag)
								require.True(t, bound.Has.Flag)
								require.NotNil(t, bound.Bill)
								require.Empty(t, *bound.Bill)
								require.True(t, bound.Has.Bill)
								require.Equal(t, "51", *bound.Rows[0].Q)
							}
							if tc.name == "string" {
								require.Nil(t, bound.Flag)
								require.False(t, bound.Has.Flag)
							}
						})
					}
					// Repeat the complete HTTP call after plans are warm; every invocation
					// must perform one handler call and preserve converted body state.
					beforeWarm := calls
					beforePrepared := representedJSONXPreparations.Load()
					warmStart := time.Now()
					for i := 0; i < 20; i++ {
						wire := rpc(schema.MethodToolsCall, `{"q":50,"flag":true}`)
						var result struct {
							Error  any
							Result struct{ IsError bool }
						}
						require.NoError(t, json.Unmarshal(wire, &result))
						require.Nil(t, result.Error, string(wire))
						require.False(t, result.Result.IsError, string(wire))
						require.Equal(t, "50", *bound.Q)
					}
					require.Equal(t, beforeWarm+20, calls)
					require.Equal(t, beforePrepared, representedJSONXPreparations.Load(), "warm invocations reuse prepared representation metadata")
					t.Logf("warm full HTTP tools/call mean: %s", time.Since(warmStart)/20)
					// REST keeps the native JSONX owner and its existing lenient unknown policy.
					raw := `{"q":50,"rows":[{"q":"51","unexpected":true}]}`
					if named {
						raw = `{"payload":` + raw + `}`
					}
					before := calls
					response := httptest.NewRecorder()
					restRequest := httptest.NewRequest("POST", "/strict-jsonx", strings.NewReader(raw))
					restRequest.Header.Set("X-Representation", strconv.FormatBool(represented))
					gateway.NewHandler(runtime, nil, "test").ServeHTTP(response, restRequest)
					require.Equal(t, 200, response.Code, response.Body.String())
					require.Equal(t, before+1, calls)
					require.Equal(t, "50", *bound.Q)
					require.Equal(t, "51", *bound.Rows[0].Q)
				})
			}
		}
	}
}
