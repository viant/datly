package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/mcp-protocol/schema"
	protocolserver "github.com/viant/mcp-protocol/server"
)

func TestHandlerListsRegistryMetadataDeterministically(t *testing.T) {
	registry := protocolserver.NewRegistry()
	for _, name := range []string{"zeta", "alpha", "middle"} {
		registry.RegisterTool(&protocolserver.ToolEntry{Metadata: schema.Tool{Name: name}})
	}
	for _, uri := range []string{"datly://localhost/zeta", "datly://localhost/alpha"} {
		registry.RegisterResource(schema.Resource{Name: uri, Uri: uri}, nil)
	}
	for _, uri := range []string{"datly://localhost/zeta/{id}", "datly://localhost/alpha/{id}"} {
		registry.RegisterResourceTemplate(schema.ResourceTemplate{Name: uri, UriTemplate: uri}, nil)
	}
	factory, err := NewHandler(&handlerService{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := actual.(*Handler)
	handler.ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: schema.LatestProtocolVersion}

	tools, protocolErr := handler.ListTools(context.Background(), nil)
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	toolNames := make([]string, len(tools.Tools))
	for i, tool := range tools.Tools {
		toolNames[i] = tool.Name
	}
	if !reflect.DeepEqual(toolNames, []string{"alpha", "middle", "zeta"}) {
		t.Fatalf("tools=%v", toolNames)
	}

	resources, protocolErr := handler.ListResources(context.Background(), nil)
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if actual := []string{resources.Resources[0].Uri, resources.Resources[1].Uri}; !reflect.DeepEqual(actual, []string{"datly://localhost/alpha", "datly://localhost/zeta"}) {
		t.Fatalf("resources=%v", actual)
	}

	templates, protocolErr := handler.ListResourceTemplates(context.Background(), nil)
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if actual := []string{templates.ResourceTemplates[0].UriTemplate, templates.ResourceTemplates[1].UriTemplate}; !reflect.DeepEqual(actual, []string{"datly://localhost/alpha/{id}", "datly://localhost/zeta/{id}"}) {
		t.Fatalf("templates=%v", actual)
	}
}
