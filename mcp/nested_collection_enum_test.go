package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	xcontract "github.com/viant/xdatly/contract"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type nestedCollectionDeal struct {
	Channels []string                         `json:"channels" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO" choice:"NATIVE" choice:"AUDIO" choice:"DOOH" choice:"CTV" choice:"LTV" choice:"RADIO"`
	NoNull   []string                         `json:"noNull,omitempty" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" format:"nullable=false"`
	Has      *struct{ Channels, NoNull bool } `json:"-" setMarker:"true"`
}

// Match the existing producer-owned enum policy: explicit MCP enum conversion
// is retained for tools and removed from the REST storage representation.
var nestedCollectionStoragePolicy = &xcontract.Policy{Name: "nested-collection-storage:v1", Field: func(_ string, _ reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
	if field.Tag.Get("mcpEnum") != "true" {
		return nil, nil
	}
	return field.RemoveTag("choice").RemoveTag("enumAccept"), nil
}}

func (*nestedCollectionDeal) Contract(ctx context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	if _, mcp := xmcp.LookupContext(ctx); mcp {
		return nil, nil
	}
	return nestedCollectionStoragePolicy, nil
}

type nestedCollectionBody struct {
	Deals []nestedCollectionDeal `json:"deals"`
}
type nestedCollectionNamed struct {
	Body *nestedCollectionBody `parameter:",kind=body,in=payload"`
}
type nestedCollectionFlat struct {
	Body *nestedCollectionBody `parameter:",kind=body" anonymous:"true"`
}

func TestNestedMCPCollectionEnumNativeTransportAndREST(t *testing.T) {
	for _, strict := range []bool{false, true} {
		for _, named := range []bool{false, true} {
			t.Run(fmt.Sprintf("strict=%v/named=%v", strict, named), func(t *testing.T) {
				inputType := reflect.TypeFor[nestedCollectionFlat]()
				if named {
					inputType = reflect.TypeFor[nestedCollectionNamed]()
				}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "nested_collection"}, Routes: []*spec.Route{{Method: "POST", Path: "/nested-collection", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "nested_collection", StrictArguments: &strict}}}}}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: reflect.TypeFor[nestedCollectionBody]()})
				require.NoError(t, err)
				var calls atomic.Int64
				var present atomic.Bool
				record := func(body *nestedCollectionBody) {
					calls.Add(1)
					if body != nil && len(body.Deals) > 0 {
						present.Store(body.Deals[0].Has != nil && body.Deals[0].Has.Channels)
					}
				}
				var registered registry.RegisteredComponent
				if named {
					registered.Handler = custom.NewFunc(func(_ context.Context, in *nestedCollectionNamed) (*nestedCollectionBody, error) {
						record(in.Body)
						return in.Body, nil
					})
				} else {
					registered.Handler = custom.NewFunc(func(_ context.Context, in *nestedCollectionFlat) (*nestedCollectionBody, error) {
						record(in.Body)
						return in.Body, nil
					})
				}
				entry, err := artifact.Registration(registered)
				require.NoError(t, err)
				runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
				service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: runtime})
				require.NoError(t, err)
				client := mcpclient.New(t, service, schema.LatestProtocolVersion)
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				listed, err := client.ListTools(ctx, nil)
				require.NoError(t, err)
				require.Len(t, listed.Tools, 1)
				deals := listed.Tools[0].InputSchema.Properties["deals"]
				if named {
					deals = listed.Tools[0].InputSchema.Properties["body"]["properties"].(map[string]any)["deals"].(map[string]any)
				}
				channels := deals["items"].(map[string]any)["properties"].(map[string]any)["channels"].(map[string]any)
				require.Equal(t, []any{"DISPLAY", "VIDEO", "NATIVE", "AUDIO", "DOOH", "CTV", "LTV", "RADIO"}, channels["items"].(map[string]any)["enum"])
				require.NotNil(t, listed.Tools[0].OutputSchema)
				outputChannels := listed.Tools[0].OutputSchema.Properties["deals"]["items"].(map[string]any)["properties"].(map[string]any)["channels"].(map[string]any)
				require.Equal(t, []any{"DISPLAY", "VIDEO", "NATIVE", "AUDIO", "DOOH", "CTV", "LTV", "RADIO"}, outputChannels["items"].(map[string]any)["enum"])
				args := func(body map[string]any) map[string]any {
					if named {
						return map[string]any{"body": body}
					}
					return body
				}
				for _, tc := range []struct {
					name  string
					body  map[string]any
					valid bool
				}{
					{"valid", map[string]any{"deals": []any{map[string]any{"channels": []any{"DISPLAY", "CTV"}}}}, true},
					{"empty", map[string]any{"deals": []any{map[string]any{"channels": []any{}}}}, true},
					{"null", map[string]any{"deals": []any{map[string]any{"channels": nil}}}, true},
					{"omitted", map[string]any{"deals": []any{map[string]any{}}}, true},
					{"unknown member", map[string]any{"deals": []any{map[string]any{"channels": []any{"DISPLAY"}, "extra": true}}}, !strict},
					{"nonnull null", map[string]any{"deals": []any{map[string]any{"noNull": nil}}}, false},
					{"nonnull valid", map[string]any{"deals": []any{map[string]any{"noNull": []any{"DISPLAY"}}}}, true},
					{"unknown", map[string]any{"deals": []any{map[string]any{"channels": []any{"UNKNOWN"}}}}, false},
					{"null item", map[string]any{"deals": []any{map[string]any{"channels": []any{nil}}}}, false},
					{"number item", map[string]any{"deals": []any{map[string]any{"channels": []any{1}}}}, false},
					{"string", map[string]any{"deals": []any{map[string]any{"channels": "DISPLAY"}}}, false},
					{"object", map[string]any{"deals": []any{map[string]any{"channels": map[string]any{}}}}, false},
				} {
					t.Run(tc.name, func(t *testing.T) {
						before := calls.Load()
						result, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: "nested_collection", Arguments: args(tc.body)})
						failed := err != nil || result == nil || result.IsError != nil && *result.IsError
						require.Equal(t, !tc.valid, failed, "result=%+v err=%v", result, err)
						if tc.valid {
							require.Equal(t, before+1, calls.Load())
							if tc.name == "valid" || tc.name == "empty" || tc.name == "null" {
								require.True(t, present.Load(), "native nested presence")
							}
							if tc.name == "omitted" {
								require.False(t, present.Load(), "omitted member must remain absent")
							}
							if tc.name == "valid" {
								raw, e := json.Marshal(result.StructuredContent)
								require.NoError(t, e)
								require.JSONEq(t, `{"deals":[{"channels":["DISPLAY","CTV"]}]}`, string(raw))
							}
						} else {
							require.Equal(t, before, calls.Load())
						}
					})
				}
				var wg sync.WaitGroup
				failures := make(chan error, 8)
				for i := 0; i < 8; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						result, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: "nested_collection", Arguments: args(map[string]any{"deals": []any{map[string]any{"channels": []any{"VIDEO"}}}})})
						if err != nil {
							failures <- err
						} else if result == nil || result.IsError != nil && *result.IsError {
							failures <- fmt.Errorf("result=%+v", result)
						}
					}()
				}
				wg.Wait()
				close(failures)
				for err := range failures {
					require.NoError(t, err)
				}
				raw := `{"deals":[{"channels":["UNKNOWN"]}]}`
				if named {
					raw = `{"payload":` + raw + `}`
				}
				response := httptest.NewRecorder()
				gateway.NewHandler(runtime, nil, "test").ServeHTTP(response, httptest.NewRequest("POST", "/nested-collection", strings.NewReader(raw)))
				require.Equal(t, 200, response.Code, response.Body.String())
				require.JSONEq(t, `{"deals":[{"channels":["UNKNOWN"]}]}`, response.Body.String())
			})
		}
	}
}

type directLiteralCollectionBody struct {
	Channels []string `json:"channels" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO"`
}
type directLiteralCollectionInput struct {
	Body  *directLiteralCollectionBody `parameter:",kind=body" anonymous:"true"`
	Query []string                     `parameter:",kind=query,in=query" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO"`
	Path  []string                     `parameter:",kind=path,in=path" mcpEnum:"true" choice:"DISPLAY" choice:"VIDEO"`
}

type directLiteralCollectionNamedInput struct {
	Named []string `parameter:",kind=body,in=named" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO"`
	Query []string `parameter:",kind=query,in=query" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY" choice:"VIDEO"`
	Path  []string `parameter:",kind=path,in=path" mcpEnum:"true" choice:"DISPLAY" choice:"VIDEO"`
}

func (*directLiteralCollectionNamedInput) Contract(ctx context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	if _, mcp := xmcp.LookupContext(ctx); mcp {
		return nil, nil
	}
	return nestedCollectionStoragePolicy, nil
}

func (*directLiteralCollectionInput) Contract(ctx context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	if _, mcp := xmcp.LookupContext(ctx); mcp {
		return nil, nil
	}
	return nestedCollectionStoragePolicy, nil
}

type directLiteralCollectionResult struct {
	Channels []string `json:"channels"`
	Named    []string `json:"named"`
	Query    []string `json:"query"`
	Path     []string `json:"path"`
}

func TestExplicitDirectCollectionEnumNamedFlattenedQuery(t *testing.T) {
	for _, strict := range []bool{false, true} {
		for _, named := range []bool{false, true} {
			t.Run(fmt.Sprintf("strict=%v/named=%v", strict, named), func(t *testing.T) {
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "direct_literal_collection"}, Routes: []*spec.Route{{Method: "POST", Path: "/direct-literal-collection/{path}", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "direct_literal_collection", StrictArguments: &strict}}}}}
				inputType := reflect.TypeFor[directLiteralCollectionInput]()
				bodyName := "channels"
				if named {
					inputType = reflect.TypeFor[directLiteralCollectionNamedInput]()
					bodyName = "named"
				}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: reflect.TypeFor[directLiteralCollectionResult]()})
				require.NoError(t, err)
				var calls atomic.Int64
				var registration registry.RegisteredComponent
				if named {
					registration.Handler = custom.NewFunc(func(_ context.Context, in *directLiteralCollectionNamedInput) (*directLiteralCollectionResult, error) {
						calls.Add(1)
						return &directLiteralCollectionResult{Named: in.Named, Query: in.Query, Path: in.Path}, nil
					})
				} else {
					registration.Handler = custom.NewFunc(func(_ context.Context, in *directLiteralCollectionInput) (*directLiteralCollectionResult, error) {
						calls.Add(1)
						result := &directLiteralCollectionResult{Query: in.Query, Path: in.Path}
						if in.Body != nil {
							result.Channels = in.Body.Channels
						}
						return result, nil
					})
				}
				entry, err := artifact.Registration(registration)
				require.NoError(t, err)
				runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
				service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: runtime})
				require.NoError(t, err)
				client := mcpclient.New(t, service, schema.LatestProtocolVersion)
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				tools, err := client.ListTools(ctx, nil)
				require.NoError(t, err)
				require.Len(t, tools.Tools, 1)
				for _, name := range []string{bodyName, "query", "path"} {
					property := tools.Tools[0].InputSchema.Properties[name]
					require.Equal(t, []any{"DISPLAY", "VIDEO"}, property["items"].(map[string]any)["enum"])
				}
				valid := map[string]any{bodyName: []any{"DISPLAY"}, "query": []any{"DISPLAY"}, "path": []any{"DISPLAY"}}
				result, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: "direct_literal_collection", Arguments: valid})
				require.NoError(t, err)
				require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
				raw, err := json.Marshal(result.StructuredContent)
				require.NoError(t, err)
				expected := map[string]any{"channels": nil, "named": nil, "query": []string{"DISPLAY"}, "path": []string{"DISPLAY"}}
				expected[bodyName] = []string{"DISPLAY"}
				want, err := json.Marshal(expected)
				require.NoError(t, err)
				require.JSONEq(t, string(want), string(raw))
				for _, name := range []string{bodyName, "query", "path"} {
					before := calls.Load()
					arguments := map[string]any{}
					for key, value := range valid {
						arguments[key] = value
					}
					arguments[name] = []any{"UNKNOWN"}
					result, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: "direct_literal_collection", Arguments: arguments})
					require.True(t, err != nil || result != nil && result.IsError != nil && *result.IsError, "%s", name)
					require.Equal(t, before, calls.Load())
				}
				requestBody, err := json.Marshal(map[string]any{bodyName: []string{"OTHER"}})
				require.NoError(t, err)
				response := httptest.NewRecorder()
				gateway.NewHandler(runtime, nil, "test").ServeHTTP(response, httptest.NewRequest("POST", "/direct-literal-collection/OTHER?query=OTHER", strings.NewReader(string(requestBody))))
				require.Equal(t, 200, response.Code, response.Body.String())
				expected = map[string]any{"channels": nil, "named": nil, "query": []string{"OTHER"}, "path": []string{"OTHER"}}
				expected[bodyName] = []string{"OTHER"}
				want, err = json.Marshal(expected)
				require.NoError(t, err)
				require.JSONEq(t, string(want), response.Body.String())
			})
		}
	}
}

func TestExplicitLiteralCollectionPathRemainsCodecOwned(t *testing.T) {
	type input struct {
		Channels []string `parameter:",kind=path,in=channels" mcpEnum:"true" enumAccept:"literal" choice:"DISPLAY"`
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "explicit_collection_path"}, Routes: []*spec.Route{{Method: "GET", Path: "/channels/{channels}"}}}
	_, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input]()})
	require.ErrorContains(t, err, "conflicts with its explicit codec")
}
