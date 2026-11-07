package server

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	xcontract "github.com/viant/xdatly/contract"
)

type headerFixtureInput struct {
	Locale string   `parameter:",kind=header,in=Accept-Language"`
	Tags   []string `parameter:",kind=header,in=X-Test-Tag"`
}
type headerFixtureOutput struct {
	Data    string   `json:"data"`
	Metrics string   `json:"metrics"`
	Locale  string   `json:"locale"`
	Tags    []string `json:"tags"`
}

func (*headerFixtureOutput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var input struct {
		Include bool `parameter:",kind=header,in=X-Include-Metrics"`
	}
	if err := binder.Bind(ctx, &input); err != nil {
		return nil, err
	}
	if input.Include {
		return nil, nil
	}
	return &xcontract.Policy{Name: "metrics:hidden:r1", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[headerFixtureOutput]() && field.Name == "Metrics" {
			return field.Hide(), nil
		}
		return nil, nil
	}}, nil
}

// Uses the real HTTP MCP transport, dynamic contract, native binder and encoder.
// Alternating headers on one session must affect both discovery and execution,
// without retaining the previous call's metrics or language selection.
func TestHTTPHeadersReachContractAndInvocationPerRequest(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "HeaderFixture"}, Routes: []*spec.Route{{Method: "GET", Path: "/header-fixture", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "HeaderFixture"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[headerFixtureInput](), OutputType: reflect.TypeFor[headerFixtureOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *headerFixtureInput) (*headerFixtureOutput, error) {
		return &headerFixtureOutput{Data: "unchanged", Metrics: "optional-diagnostics", Locale: in.Locale, Tags: in.Tags}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime})
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{schema.LegacyProtocolVersion, schema.LatestProtocolVersion} {
		t.Run(version, func(t *testing.T) {
			server := startWireServerConfig(t, Config{Service: service, ProtocolVersion: version, Transport: TransportConfig{Kind: TransportStreamable}})
			session := ""
			if version == schema.LegacyProtocolVersion {
				session = initializeWireSession(t, server.baseURL)
			}
			for index, value := range []string{"", "true", "false", "true", ""} {
				headers := http.Header{"Mcp-Protocol-Version": {version}}
				if value != "" {
					headers.Set("X-Include-Metrics", value)
					headers.Set("Accept-Language", "fr")
					headers["X-Test-Tag"] = []string{"one", "two"}
				}
				for _, method := range []string{schema.MethodToolsList, schema.MethodToolsCall} {
					params := map[string]interface{}{}
					if version == schema.LatestProtocolVersion {
						params["_meta"] = map[string]interface{}{"io.modelcontextprotocol/protocolVersion": version, "io.modelcontextprotocol/clientCapabilities": map[string]interface{}{}, "io.modelcontextprotocol/clientInfo": map[string]interface{}{"name": "header-test", "version": "1"}}
					}
					headers.Del(schema.HeaderName)
					if method == schema.MethodToolsCall {
						if version == schema.LatestProtocolVersion {
							headers.Set(schema.HeaderName, "HeaderFixture")
						}
						params["name"] = "HeaderFixture"
						params["arguments"] = map[string]interface{}{}
					}
					response, _, status := postWireRequest(t, server.baseURL+"/mcp", map[string]interface{}{"jsonrpc": "2.0", "id": index + 2, "method": method, "params": params}, session, "", headers)
					if status != http.StatusOK || response.Error != nil {
						t.Fatalf("%s metrics=%q: status=%d error=%+v", method, value, status, response.Error)
					}
					if method == schema.MethodToolsList {
						var list schema.ListToolsResult
						if err := json.Unmarshal(response.Result, &list); err != nil {
							t.Fatal(err)
						}
						if len(list.Tools) != 1 {
							t.Fatalf("tools=%+v", list.Tools)
						}
						raw, _ := json.Marshal(list.Tools[0].OutputSchema)
						var output map[string]interface{}
						if err := json.Unmarshal(raw, &output); err != nil {
							t.Fatal(err)
						}
						props, ok := output["properties"].(map[string]interface{})
						if !ok {
							t.Fatalf("schema=%s", raw)
						}
						_, metrics := props["metrics"]
						if metrics != (value == "true") {
							t.Fatalf("metrics=%q schema=%s", value, raw)
						}
					} else {
						var result struct {
							IsError    bool                   `json:"isError"`
							Structured map[string]interface{} `json:"structuredContent"`
						}
						if err := json.Unmarshal(response.Result, &result); err != nil {
							t.Fatal(err)
						}
						if result.IsError || result.Structured["data"] != "unchanged" {
							t.Fatalf("result=%s", response.Result)
						}
						_, metrics := result.Structured["metrics"]
						if metrics != (value == "true") {
							t.Fatalf("metrics=%q result=%s", value, response.Result)
						}
						if value != "" {
							if result.Structured["locale"] != "fr" || !reflect.DeepEqual(result.Structured["tags"], []interface{}{"one", "two"}) {
								t.Fatalf("other headers lost: %s", response.Result)
							}
						} else if locale, _ := result.Structured["locale"].(string); locale != "" {
							t.Fatalf("prior request leaked: %s", response.Result)
						}
					}
				}
			}

		})
	}
}
