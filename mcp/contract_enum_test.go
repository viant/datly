package mcp_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/scy/auth/jwt"
	xcontract "github.com/viant/xdatly/contract"
)

type enumContractView struct {
	Code  int    `json:"code" choice:"0:draft" choice:"10:active"`
	Label string `json:"label" choice:"0:draft" choice:"10:active"`
}

func (*enumContractView) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Claims *jwt.Claims `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	mode := state.Claims.Subject
	if mode == "stored" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "enum/" + mode, Field: func(_ string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if parent != reflect.TypeFor[enumContractView]() {
			return nil, nil
		}
		switch f.Name {
		case "Code", "Label":
			accept, style := "literal", "ordinal"
			if mode == "cross" {
				if f.Name == "Code" {
					accept, style = "literal", "literal"
				} else {
					accept, style = "ordinal", "ordinal"
				}
			}
			return f.Clone().SetEnum("10:active").SetTag("enumAccept", accept).SetTag("enumStyle", style), nil
		}
		return nil, nil
	}}, nil
}

type enumContractInput struct {
	Body *enumContractView `parameter:",kind=body,in=body,required=true" json:"body"`
}
type enumContractOutput struct {
	Data *enumContractView `json:"data"`
}

func TestContractEnumWireTypesDifferFromStorageAcrossMCPAndOpenAPI(t *testing.T) {
	key := testharness.NewJWT(t)
	factory, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "EnumContract"}, Routes: []*spec.Route{{Method: "POST", Path: "/enum", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "EnumContract"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[enumContractInput](), OutputType: reflect.TypeFor[enumContractOutput](), CodecFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *enumContractInput) (*enumContractOutput, error) {
		calls++
		if in.Body.Code != 10 || in.Body.Label != "active" {
			t.Errorf("wire values not converted to storage: %+v", in.Body)
		}
		return &enumContractOutput{Data: in.Body}, nil
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
	handlerFactory, err := mcpserver.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := handlerFactory(t.Context(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.(*mcpserver.Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: "2025-11-25"}
	for _, mode := range []string{"cross", "split", "stored", "cross"} {
		token := key.Sign(t, mode, time.Now().Add(time.Hour))
		ctx := context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: token})
		list, rpcErr := handler.ListTools(ctx, nil)
		if rpcErr != nil {
			t.Fatal(rpcErr)
		}
		input := list.Tools[0].InputSchema.Properties["body"]["properties"].(map[string]any)
		out := list.Tools[0].OutputSchema.Properties["data"]["properties"].(map[string]any)
		inputKinds, outputKinds := map[string]string{"code": "string", "label": "integer"}, map[string]string{"code": "string", "label": "integer"}
		args := map[string]any{"code": "active", "label": 10}
		if mode == "split" {
			inputKinds["label"] = "string"
			outputKinds["code"] = "integer"
			args["label"] = "active"
		}
		if mode == "stored" {
			inputKinds = map[string]string{"code": "integer", "label": "string"}
			outputKinds = inputKinds
			args = map[string]any{"code": 10, "label": "active"}
		}
		scalarType := func(v any) string {
			if s, ok := v.(string); ok {
				return s
			}
			if a, ok := v.([]string); ok {
				return a[0]
			}
			t.Fatalf("unknown schema type %T %v", v, v)
			return ""
		}
		for field, want := range inputKinds {
			if scalarType(input[field].(map[string]any)["type"]) != want {
				t.Fatalf("%s input %s: %+v", mode, field, input)
			}
		}
		for field, want := range outputKinds {
			if scalarType(out[field].(map[string]any)["type"]) != want {
				t.Fatalf("%s output %s: %+v", mode, field, out)
			}
		}
		assertDomain := func(value any, kind string) {
			var expected any = []any{"active"}
			if kind == "integer" {
				expected = []any{int64(10)}
			}
			if mode == "stored" {
				if kind == "integer" {
					expected = []any{int64(0), int64(10)}
				} else {
					expected = []any{"draft", "active"}
				}
			}
			actualJSON, _ := json.Marshal(value)
			expectedJSON, _ := json.Marshal(expected)
			if string(actualJSON) != string(expectedJSON) {
				t.Fatalf("%s enum=%s want=%s", mode, actualJSON, expectedJSON)
			}
		}
		for field, kind := range inputKinds {
			assertDomain(input[field].(map[string]any)["enum"], kind)
		}
		for field, kind := range outputKinds {
			assertDomain(out[field].(map[string]any)["enum"], kind)
		}
		call := &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "EnumContract", Arguments: map[string]any{"body": args}}}
		result, rpcErr := handler.CallTool(ctx, &jsonrpc.TypedRequest[*schema.CallToolRequest]{Request: call})
		if rpcErr != nil || result == nil || result.IsError != nil && *result.IsError {
			t.Fatalf("%s: %+v %v", mode, result, rpcErr)
		}
		encoded, _ := json.Marshal(result.StructuredContent)
		var wire map[string]any
		_ = json.Unmarshal(encoded, &wire)
		data := wire["data"].(map[string]any)
		for field, kind := range outputKinds {
			var want any = "active"
			if kind == "integer" {
				want = float64(10)
			}
			if data[field] != want {
				t.Fatalf("%s %s output=%v want=%v", mode, field, data[field], want)
			}
		}
		req := httptest.NewRequest("GET", "/openapi.json", nil)
		req.Header.Set("Authorization", token)
		scope, err := request.New(req)
		if err != nil {
			t.Fatal(err)
		}
		document, err := (openapi.Generator{}).Generate(t.Context(), openapi.Request{Info: openapi3.Info{Title: "enum", Version: "1"}, Components: []*registry.RegisteredComponent{registered}, Contracts: runtime, Scope: scope})
		scope.Close()
		if err != nil {
			t.Fatal(err)
		}
		var deref func(*openapi3.Schema) *openapi3.Schema
		deref = func(s *openapi3.Schema) *openapi3.Schema {
			if len(s.AnyOf) > 0 {
				return deref(s.AnyOf[0])
			}
			if s.Ref != "" {
				return deref(document.Components.Schemas[strings.TrimPrefix(s.Ref, "#/components/schemas/")])
			}
			return s
		}
		operation := document.Paths["/enum"].Post
		body := deref(operation.RequestBody.Content["application/json"].Schema)
		if child := body.Properties["body"]; child != nil {
			body = deref(child)
		}
		response := deref(operation.Responses["200"].Content["application/json"].Schema)
		response = deref(response.Properties["data"])
		for field, want := range inputKinds {
			assertDomain(deref(body.Properties[field]).Enum, want)
			if deref(body.Properties[field]).Type != want {
				t.Fatalf("OpenAPI %s input %s: %+v", mode, field, body.Properties[field])
			}
		}
		for field, want := range outputKinds {
			assertDomain(deref(response.Properties[field]).Enum, want)
			if deref(response.Properties[field]).Type != want {
				t.Fatalf("OpenAPI %s output %s: %+v", mode, field, response.Properties[field])
			}
		}
	}
	if calls != 4 {
		t.Fatal(calls)
	}
}
