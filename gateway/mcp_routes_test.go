package gateway

import (
	"context"
	"testing"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/version"
	"github.com/viant/mcp-protocol/server"
)

func TestContractMCPExplicitMethodRoutes(t *testing.T) {
	component := contractTestComponent(t)
	registry := server.NewRegistry()
	router := &Router{mcpRegistry: registry}
	mappings := map[string]*contract.Meta{
		"POST:/campaigns":  {Name: "CreateCampaign"},
		"PATCH:/campaigns": {Name: "PatchCampaign"},
	}
	item := &dpath.Item{}
	for _, method := range []string{"POST", "PATCH"} {
		item.Paths = append(item.Paths, &dpath.Path{Path: contract.Path{Method: method, URI: "/campaigns"}, ModelContextProtocol: contract.ModelContextProtocol{MCPRoutes: mappings}})
	}
	for _, path := range item.Paths {
		provider := repository.NewProvider(path.Path, &version.Control{}, func(context.Context, ...repository.Option) (*repository.Component, error) { return component, nil })
		if err := router.buildToolsIntegration(item, path, &Route{Path: &path.Path}, provider); err != nil {
			t.Fatal(err)
		}
	}
	tools, err := registry.ListToolsFor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 {
		t.Fatalf("%+v", tools)
	}
	for _, tool := range tools {
		body := tool.InputSchema.Properties["Body"]["items"].(map[string]any)
		mode := body["properties"].(map[string]any)["mode"].(map[string]any)
		want := "integer"
		if tool.Name == "CreateCampaign" {
			want = "string"
		}
		if mode["type"] != want {
			t.Fatalf("%s %+v", tool.Name, mode)
		}
	}
	mappings["DELETE:/campaigns"] = &contract.Meta{Name: "DeleteCampaign"}
	if _, err = declaredMCPRoute(item, item.Paths[0]); err == nil {
		t.Fatal("accepted undeclared method")
	}
}
