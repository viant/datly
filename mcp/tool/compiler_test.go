package tool

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type toolNested struct {
	When time.Time `json:"when"`
	Tags []string  `json:"tags"`
}

type toolInput struct {
	Search  []string `json:"search_terms"`
	ID      int
	Payload toolNested `json:"payload"`
	Hidden  string     `json:"-"`
	Request *http.Request
}

type anonymousPayload struct {
	Name   string `json:"name"`
	Count  int    `json:"count,omitempty"`
	Hidden string `json:"-"`
}

type anonymousInput struct {
	Payload anonymousPayload `anonymous:"true"`
}

type anonymousMCPPayload struct {
	Name   string `json:"name" mcp:"name=Name,aliases=legacy_name"`
	Count  int    `json:"count"`
	Hidden string `json:"hidden" mcp:"-"`
}

type anonymousMCPInput struct {
	Payload anonymousMCPPayload `anonymous:"true"`
}

type mcpTaggedInput struct {
	Request       toolNested `json:"diagnose,omitempty" mcp:"name=Request,aliases=diagnose"`
	Debug         bool       `json:"debug,omitempty" mcp:"name=Debug,aliases=debug"`
	ForwardedHost string     `parameter:",kind=header,in=X-Forwarded-Host" json:"forwardedHost,omitempty" mcp:"-"`
}

func TestCompilerBuildsSchemaAndBindingFromRouteContract(t *testing.T) {
	required := true
	description := &spec.Parameter{Name: "Search", Description: "CSV search terms", Example: "one,two"}
	bindings := []bindly.BindingSpec{
		{Path: "Search", SourceType: reflect.TypeOf(""), Name: "Search", Location: bindstate.Location{Kind: "query", In: "q"}, Required: &required, Extension: description},
		{Path: "ID", Name: "Identifier", Location: bindstate.Location{Kind: "path", In: "id"}},
		{Path: "Payload", Location: bindstate.Location{Kind: "body", In: "payload"}},
		{Path: "Hidden", Location: bindstate.Location{Kind: "header", In: "X-Hidden"}},
		{Path: "Request", Location: bindstate.Location{Kind: "http_request"}},
	}
	contract := testRouteContract(t, reflect.TypeOf(toolInput{}), bindings)
	plan, err := NewCompiler().Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Search"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "search.run", Description: "Run search"},
		Contract:  contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := plan.Metadata()
	if metadata.Name != "search.run" || metadata.Description == nil || *metadata.Description != "Run search" ||
		metadata.InputSchema.Type != "object" || len(metadata.InputSchema.Properties) != 3 ||
		len(metadata.InputSchema.Required) != 1 || metadata.InputSchema.Required[0] != "search_terms" {
		t.Fatalf("metadata = %+v", metadata)
	}
	search := metadata.InputSchema.Properties["search_terms"]
	if search["type"] != "string" || search["description"] != "CSV search terms" {
		t.Fatalf("search schema = %+v", search)
	}
	if metadata.InputSchema.Properties["Identifier"]["type"] != "integer" {
		t.Fatalf("ID schema = %+v", metadata.InputSchema.Properties["Identifier"])
	}
	payload := metadata.InputSchema.Properties["payload"]
	properties, ok := payload["properties"].(map[string]interface{})
	if !ok || properties["when"].(map[string]interface{})["format"] != "date-time" ||
		properties["tags"].(map[string]interface{})["type"] != "array" || payload["required"] != nil {
		t.Fatalf("payload schema = %+v", payload)
	}
	args := plan.Arguments()
	if len(args) != 3 || args[0].PublicName() != "Identifier" || args[1].PublicName() != "payload" || args[2].PublicName() != "search_terms" ||
		args[2].SourceType() != reflect.TypeOf("") || args[2].DestinationType() != reflect.TypeOf([]string{}) {
		t.Fatalf("arguments = %+v", args)
	}
	scope, err := plan.Scope(map[string]interface{}{
		"Identifier": float64(7), "search_terms": "one,two", "payload": map[string]interface{}{"when": "2026-07-14T10:00:00Z", "tags": []interface{}{"a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	searchValue, ok, err := scope.Query().Locate(nil).Value(context.Background(), reflect.TypeOf(""), "q")
	if err != nil || !ok || searchValue != "one,two" {
		t.Fatalf("search provider = %#v, %v, %v", searchValue, ok, err)
	}
	if _, err = plan.Scope(map[string]interface{}{"Identifier": float64(7)}); err == nil || !strings.Contains(err.Error(), "missing required") {
		t.Fatalf("required argument error = %v", err)
	}
}

func TestCompilerUsesMCPNamesAliasesAndExclusionsWithoutChangingBindingSources(t *testing.T) {
	required := true
	contract := testRouteContract(t, reflect.TypeOf(mcpTaggedInput{}), []bindly.BindingSpec{
		{Path: "Request", Location: bindstate.Location{Kind: "body", In: "diagnose"}, Required: &required},
		{Path: "Debug", Location: bindstate.Location{Kind: "query", In: "debug"}},
		{Path: "ForwardedHost", Location: bindstate.Location{Kind: "header", In: "X-Forwarded-Host"}},
	})
	plan, err := NewCompiler().Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Diagnostic"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "diagnostic.run"}, Contract: contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := plan.Metadata()
	if len(metadata.InputSchema.Properties) != 2 || metadata.InputSchema.Properties["Request"] == nil || metadata.InputSchema.Properties["Debug"] == nil ||
		metadata.InputSchema.Properties["diagnose"] != nil || metadata.InputSchema.Properties["ForwardedHost"] != nil {
		t.Fatalf("schema = %+v", metadata.InputSchema.Properties)
	}
	scope, err := plan.Scope(map[string]interface{}{
		"diagnose": map[string]interface{}{"when": "2026-07-14T10:00:00Z", "tags": []interface{}{"a"}},
		"debug":    false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := scope.Body().Locate(nil).Value(context.Background(), reflect.TypeOf(toolNested{}), "diagnose"); err != nil || !ok {
		t.Fatalf("body diagnose lookup ok=%v err=%v", ok, err)
	}
	debug, ok, err := scope.Query().Locate(nil).Value(context.Background(), reflect.TypeOf(false), "debug")
	if err != nil || !ok || debug != "false" {
		t.Fatalf("debug lookup = %#v ok=%v err=%v", debug, ok, err)
	}
	for _, args := range []map[string]interface{}{
		{"Request": map[string]interface{}{"when": "2026-07-14T10:00:00Z"}, "diagnose": map[string]interface{}{"when": "2026-07-14T10:00:00Z"}},
		{"ForwardedHost": "example.com", "Request": map[string]interface{}{"when": "2026-07-14T10:00:00Z"}},
	} {
		if _, err := plan.Scope(args); err == nil {
			t.Fatalf("expected rejected MCP arguments: %+v", args)
		}
	}
}

func TestCompilerRejectsInvalidPublicContracts(t *testing.T) {
	required := true
	tests := []struct {
		name     string
		typeOf   reflect.Type
		bindings []bindly.BindingSpec
		exposure *spec.MCPExposure
		match    string
	}{
		{name: "invalid tool name", typeOf: reflect.TypeOf(struct{ ID int }{}), bindings: []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "query", In: "id"}}}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "bad name"}, match: "unsupported character"},
		{name: "required hidden", typeOf: reflect.TypeOf(struct {
			Value string `json:"-"`
		}{}), bindings: []bindly.BindingSpec{{Path: "Value", Location: bindstate.Location{Kind: "query", In: "value"}, Required: &required}}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "hidden"}, match: "hidden"},
		{name: "required request", typeOf: reflect.TypeOf(struct{ Request *http.Request }{}), bindings: []bindly.BindingSpec{{Path: "Request", Location: bindstate.Location{Kind: "http_request"}, Required: &required}}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "request"}, match: "cannot be supplied"},
		{name: "duplicate public", typeOf: reflect.TypeOf(struct{ A, B string }{}), bindings: []bindly.BindingSpec{{Path: "A", Name: "same", Location: bindstate.Location{Kind: "query", In: "a"}}, {Path: "B", Name: "same", Location: bindstate.Location{Kind: "query", In: "b"}}}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "duplicate"}, match: "duplicate public"},
		{name: "duplicate alias", typeOf: reflect.TypeOf(struct {
			A string `mcp:"aliases=same"`
			B string `mcp:"aliases=same"`
		}{}), bindings: []bindly.BindingSpec{{Path: "A", Name: "A", Location: bindstate.Location{Kind: "query", In: "a"}}, {Path: "B", Name: "B", Location: bindstate.Location{Kind: "query", In: "b"}}}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "duplicateAlias"}, match: "duplicate public"},
		{name: "required mcp hidden", typeOf: reflect.TypeOf(struct {
			Value string `mcp:"-"`
		}{}), bindings: []bindly.BindingSpec{{Path: "Value", Location: bindstate.Location{Kind: "query", In: "value"}, Required: &required}}, exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "hiddenMCP"}, match: "hidden from MCP"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projectionBindings := test.bindings
			if test.name == "duplicate public" {
				projectionBindings = []bindly.BindingSpec{
					{Path: "A", Name: "A", Location: bindstate.Location{Kind: "param"}},
					{Path: "B", Name: "B", Location: bindstate.Location{Kind: "param"}},
				}
			}
			contract := testRouteContractWithProjection(t, test.typeOf, test.bindings, projectionBindings)
			_, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Test"}, Exposure: test.exposure, Contract: contract})
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestCompilerFlattensExplicitAnonymousBody(t *testing.T) {
	required := true
	contract := testRouteContract(t, reflect.TypeOf(anonymousInput{}), []bindly.BindingSpec{{
		Path: "Payload", Location: bindstate.Location{Kind: "body"}, Required: &required,
	}})
	plan, err := NewCompiler().Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Anonymous"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "anonymous.run"}, Contract: contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := plan.Metadata()
	if len(metadata.InputSchema.Properties) != 2 || metadata.InputSchema.Properties["name"]["type"] != "string" ||
		metadata.InputSchema.Properties["count"]["type"] != "integer" ||
		len(metadata.InputSchema.Required) != 0 {
		t.Fatalf("anonymous schema = %+v", metadata.InputSchema)
	}
	if _, err = plan.Scope(nil); err != nil {
		t.Fatalf("anonymous subfields inferred requiredness: %v", err)
	}
	scope, err := plan.Scope(map[string]interface{}{"name": "Ada", "count": float64(3)})
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := scope.Body().Locate(nil).Value(context.Background(), reflect.TypeOf(anonymousPayload{}), "")
	if err != nil || !ok {
		t.Fatalf("anonymous body value=%#v ok=%v err=%v", value, ok, err)
	}
	payload := value.(anonymousPayload)
	if payload.Name != "Ada" || payload.Count != 3 {
		t.Fatalf("anonymous body = %+v", payload)
	}
}

func TestCompilerAnonymousMCPNamePreservesJSONBodySource(t *testing.T) {
	contract := testRouteContract(t, reflect.TypeOf(anonymousMCPInput{}), []bindly.BindingSpec{{
		Path: "Payload", Location: bindstate.Location{Kind: "body"},
	}})
	plan, err := NewCompiler().Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "AnonymousMCP"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "anonymous.mcp"}, Contract: contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := plan.Metadata()
	if len(metadata.InputSchema.Properties) != 2 || metadata.InputSchema.Properties["Name"] == nil || metadata.InputSchema.Properties["count"] == nil ||
		metadata.InputSchema.Properties["name"] != nil || metadata.InputSchema.Properties["hidden"] != nil {
		t.Fatalf("anonymous MCP schema = %+v", metadata.InputSchema.Properties)
	}
	scope, err := plan.Scope(map[string]interface{}{"legacy_name": "Ada", "count": float64(3)})
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := scope.Body().Locate(nil).Value(context.Background(), reflect.TypeOf(anonymousMCPPayload{}), "")
	if err != nil || !ok {
		t.Fatalf("anonymous MCP body value=%#v ok=%v err=%v", value, ok, err)
	}
	payload := value.(anonymousMCPPayload)
	if payload.Name != "Ada" || payload.Count != 3 {
		t.Fatalf("anonymous MCP body = %+v", payload)
	}
}

func TestCompilerTreatsAuthorizationHeaderAsProtocolOwned(t *testing.T) {
	type input struct {
		Authorization string `json:"authorization"`
	}
	required := true
	contract := testRouteContract(t, reflect.TypeOf(input{}), []bindly.BindingSpec{{
		Path: "Authorization", Location: bindstate.Location{Kind: "header", In: "authorization"}, Required: &required,
	}})
	plan, err := NewCompiler().Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Auth"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "auth.run"}, Contract: contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Arguments()) != 0 || len(plan.Metadata().InputSchema.Properties) != 0 {
		t.Fatalf("protocol-owned Authorization was exposed: %+v", plan.Metadata().InputSchema)
	}
	if _, err = plan.Scope(nil); err != nil {
		t.Fatalf("protocol-owned required header was validated as an MCP argument: %v", err)
	}
}

func TestCompilerRejectsInvalidAnonymousBodyContract(t *testing.T) {
	tests := []struct {
		name    string
		binding bindly.BindingSpec
		match   string
	}{
		{name: "non body", binding: bindly.BindingSpec{Path: "Payload", Location: bindstate.Location{Kind: "query", In: "payload"}}, match: "must use body"},
		{name: "named body", binding: bindly.BindingSpec{Path: "Payload", Location: bindstate.Location{Kind: "body", In: "payload"}}, match: "cannot use named body"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			contract := testRouteContract(t, reflect.TypeOf(anonymousInput{}), []bindly.BindingSpec{test.binding})
			_, err := NewCompiler().Compile(Input{
				Component: spec.Key{Kind: spec.KindComponent, Name: "Anonymous"},
				Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "anonymous.run"}, Contract: contract,
			})
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestPlanMetadataIsDetached(t *testing.T) {
	type input struct{ IDs []int }
	contract := testRouteContract(t, reflect.TypeOf(input{}), []bindly.BindingSpec{{Path: "IDs", Location: bindstate.Location{Kind: "query", In: "ids"}}})
	plan, err := NewCompiler().Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Test"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "test"}, Contract: contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := plan.Metadata()
	metadata.InputSchema.Properties["ids"]["type"] = "string"
	metadata.InputSchema.Properties["ids"]["items"].(map[string]interface{})["type"] = "string"
	actual := plan.Metadata().InputSchema.Properties["ids"]
	if actual["type"] != "array" || actual["items"].(map[string]interface{})["type"] != "integer" {
		t.Fatal("returned metadata aliases immutable tool plan")
	}
}

func TestPlanArgumentsAreDetached(t *testing.T) {
	type input struct {
		ID int `mcp:"aliases=legacy_id"`
	}
	contract := testRouteContract(t, reflect.TypeOf(input{}), []bindly.BindingSpec{{Path: "ID", Location: bindstate.Location{Kind: "query", In: "id"}}})
	plan, err := NewCompiler().Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Test"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "test"}, Contract: contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	arguments := plan.Arguments()
	arguments[0].aliases[0] = "mutated"
	if actual := plan.Arguments()[0].Aliases(); len(actual) != 1 || actual[0] != "legacy_id" {
		t.Fatalf("arguments aliases mutated: %+v", actual)
	}
}

func testRouteContract(t *testing.T, inputType reflect.Type, bindings []bindly.BindingSpec) *registry.RouteInputContract {
	return testRouteContractWithProjection(t, inputType, bindings, bindings)
}

func testRouteContractWithProjection(t *testing.T, inputType reflect.Type, bindings, projectionBindings []bindly.BindingSpec) *registry.RouteInputContract {
	t.Helper()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(inputType, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	route := spec.RouteRef{Method: "POST", Path: "/tools"}
	projectionPlan, err := injector.CompilePlan(inputType, projectionBindings...)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := projectionPlan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	input, err := registry.NewInputContract(inputType, projection, registry.RouteInput{Route: route, Plan: plan, Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}
	result, ok := input.ForRoute(route)
	if !ok {
		t.Fatal("route contract not found")
	}
	return result
}
