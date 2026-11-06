package mcp_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
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

type dynamicCatalogInput struct {
	*CatalogPolicyMethods
	Body   *catalogMetadataBody `parameter:",kind=body,in=body" json:"body"`
	Reason string               `parameter:",kind=query,in=reason"`
	Mode   string               `parameter:",kind=query,in=mode" choice:"draft" choice:"approved"`
	Secret string               `parameter:",kind=query,in=secret"`
}
type CatalogPolicyMethods struct{}

func (*CatalogPolicyMethods) LinkedMethod() bool { return true }

type CatalogMetadataEmbedded struct {
	Label string `json:"label"`
}

func (v *CatalogMetadataEmbedded) LinkedLabel() string { return v.Label }

type catalogMetadataLeaf struct {
	*CatalogMetadataEmbedded
	Mode   string                              `json:"mode" mcp:"name=Choice,aliases=choice"`
	Secret string                              `json:"secret"`
	Has    *struct{ Label, Mode, Secret bool } `json:"-" setMarker:"true"`
}
type catalogMetadataGroup struct {
	*CatalogMetadataEmbedded
	Rows [][]*catalogMetadataLeaf       `json:"rows"`
	ByID map[int][]*catalogMetadataLeaf `json:"byId"`
}
type catalogMetadataBody struct {
	Groups []*catalogMetadataGroup `json:"groups"`
}

type dynamicCatalogOutput struct {
	*CatalogPolicyMethods
	Public string
	Secret string
	Child  *dynamicCatalogChild
}
type dynamicCatalogChild struct {
	Visible string
	Secret  string
}

func (*dynamicCatalogInput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	return dynamicCatalogPolicy(ctx, binder)
}
func (*dynamicCatalogOutput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	return dynamicCatalogPolicy(ctx, binder)
}
func dynamicCatalogPolicy(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Claims *jwt.Claims `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	if state.Claims.Subject == "admin" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "restricted", Field: func(_ string, parentType reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		switch parentType.Name() {
		case "dynamicCatalogInput", "catalogMetadataLeaf":
			switch f.Name {
			case "Reason":
				return f.Clone().Required(), nil
			case "Secret":
				return f.Clone().Hide(), nil
			case "Mode":
				renamed := f.Clone().SetEnum("draft").SetDescription("Draft only")
				renamed.Name = "Workflow"
				return renamed, nil
			}
		case "dynamicCatalogOutput", "dynamicCatalogChild":
			switch f.Name {
			case "Secret":
				return f.Clone().Hide(), nil
			}
		}
		return nil, nil
	}}, nil
}

func TestToolsListUsesVerifiedCallerContractWithoutMutatingCatalog(t *testing.T) {
	key := testharness.NewJWT(t)
	factory, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DynamicCatalog"}, Routes: []*spec.Route{{Method: "GET", Path: "/dynamic-catalog", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "DynamicCatalog"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[dynamicCatalogInput](), OutputType: reflect.TypeFor[dynamicCatalogOutput](), CodecFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	var seen *dynamicCatalogInput
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, input *dynamicCatalogInput) (*dynamicCatalogOutput, error) {
		calls++
		seen = input
		return &dynamicCatalogOutput{Public: "public", Secret: "private", Child: &dynamicCatalogChild{Visible: "visible", Secret: "nested private"}}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{registered}, Invoker: rt})
	if err != nil {
		t.Fatal(err)
	}
	newHandler, err := mcpserver.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := newHandler(t.Context(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.(*mcpserver.Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: "2025-11-25"}
	base, _ := json.Marshal(service.Registry().ListRegisteredTools())
	for _, subject := range []string{"reader", "admin", "reader"} {
		ctx := context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: key.Sign(t, subject, time.Now().Add(time.Hour))})
		list, rpcErr := handler.ListTools(ctx, nil)
		if rpcErr != nil {
			t.Fatal(rpcErr)
		}
		if len(list.Tools) != 1 || list.TtlMs != 0 {
			t.Fatalf("list=%+v", list)
		}
		tool := list.Tools[0]
		bodySchema, _ := json.Marshal(tool.InputSchema.Properties["body"])
		if strings.Contains(string(bodySchema), `"Has"`) || strings.Contains(string(bodySchema), `"has"`) {
			t.Fatal("presence marker exposed")
		}
		if strings.Contains(string(bodySchema), `"secret"`) != (subject == "admin") {
			t.Fatalf("nested caller input visibility: %s", bodySchema)
		}
		if !strings.Contains(string(bodySchema), `"Choice"`) {
			t.Fatalf("nested MCP name missing: %s", bodySchema)
		}
		_, inputSecret := tool.InputSchema.Properties["Secret"]
		_, outputSecret := tool.OutputSchema.Properties["Secret"]
		if inputSecret != (subject == "admin") || outputSecret != (subject == "admin") {
			t.Fatalf("caller %s secret input=%v output=%v", subject, inputSecret, outputSecret)
		}
		if subject == "reader" {
			found := false
			for _, name := range tool.InputSchema.Required {
				found = found || name == "Reason"
			}
			if !found {
				t.Fatal("effective required query field not advertised")
			}
			child := tool.OutputSchema.Properties["Child"]
			encoded, _ := json.Marshal(child)
			var members map[string]any
			if err := json.Unmarshal(encoded, &members); err != nil {
				t.Fatal(err)
			}
			if props, ok := members["properties"].(map[string]any); ok {
				if _, exists := props["Secret"]; exists {
					t.Fatal("nested secret leaked")
				}
			} else {
				t.Fatalf("nested schema properties missing: %s", encoded)
			}
			values := tool.InputSchema.Properties["Workflow"]["enum"]
			raw, _ := json.Marshal(values)
			if string(raw) != `["draft"]` {
				t.Fatalf("choices=%s", raw)
			}
		}
	}
	if _, err := handler.ListTools(t.Context(), nil); err == nil {
		t.Fatal("cached schema bypassed credential binding")
	}
	after, _ := json.Marshal(service.Registry().ListRegisteredTools())
	if string(base) != string(after) || calls != 0 {
		t.Fatal("catalog mutated or business executed")
	}
	for _, subject := range []string{"reader", "admin"} {
		ctx := context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: key.Sign(t, subject, time.Now().Add(time.Hour))})
		modeName := "Mode"
		if subject == "reader" {
			modeName = "Workflow"
		}
		request := &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "DynamicCatalog", Arguments: map[string]any{"Secret": "attack", "Reason": "review", modeName: "approved"}}}
		request.Params.Arguments["body"] = map[string]any{"groups": []any{map[string]any{
			"label": "group", "rows": []any{[]any{map[string]any{"label": "leaf", "Choice": "approved", "secret": "attack", "Has": map[string]any{"Secret": true}}, nil}},
			"byId": map[string]any{"5": []any{map[string]any{"choice": "approved", "secret": "attack"}}},
		}}}
		result, rpcErr := handler.CallTool(ctx, &jsonrpc.TypedRequest[*schema.CallToolRequest]{Request: request})
		if rpcErr != nil || result == nil || result.IsError != nil && *result.IsError {
			t.Fatalf("call %s: %+v %v", subject, result, rpcErr)
		}
		encoded, _ := json.Marshal(result.StructuredContent)
		var body map[string]any
		if err = json.Unmarshal(encoded, &body); err != nil {
			t.Fatal(err)
		}
		_, secret := body["Secret"]
		if secret != (subject == "admin") {
			t.Fatalf("output %s: %s", subject, encoded)
		}
		child := body["Child"].(map[string]any)
		_, secret = child["Secret"]
		if secret != (subject == "admin") {
			t.Fatal("nested output policy bypassed")
		}
		if subject == "reader" && seen.Secret != "" {
			t.Fatal("hidden tool argument bound")
		}
		if seen.Body == nil || len(seen.Body.Groups) != 1 {
			t.Fatal("nested body was not bound")
		}
		group := seen.Body.Groups[0]
		leaf := group.Rows[0][0]
		mapped := group.ByID[5][0]
		if group.LinkedLabel() != "group" || leaf.LinkedLabel() != "leaf" || group.Rows[0][1] != nil {
			t.Fatal("linked embeddings or nil leaf lost")
		}
		for _, row := range []*catalogMetadataLeaf{leaf, mapped} {
			if subject == "admin" && row == mapped {
				if row.Mode != "approved" || row.Secret != "attack" || row.Has != nil {
					t.Fatal("metadata-free native map behavior changed")
				}
				continue
			}
			if row.Mode != "approved" || row.Has == nil || !row.Has.Mode {
				t.Fatalf("nested MCP normalization/presence lost for %s: mode=%q has=%+v", subject, row.Mode, row.Has)
			}
			if subject == "reader" && (row.Secret != "" || row.Has.Secret) {
				t.Fatal("nested hidden field or forged marker admitted")
			}
			if subject == "admin" && (row.Secret != "attack" || !row.Has.Secret) {
				t.Fatal("unchanged linked decoder lost presence")
			}
		}
		if seen.Mode != "approved" {
			t.Fatal("enum metadata installed unrequested validation")
		}
		before := calls
		request.Params.Arguments = map[string]any{}
		result, rpcErr = handler.CallTool(ctx, &jsonrpc.TypedRequest[*schema.CallToolRequest]{Request: request})
		if subject == "reader" && (calls != before || rpcErr == nil && (result.IsError == nil || !*result.IsError)) {
			t.Fatal("dynamic required field was not enforced")
		}
	}
}
