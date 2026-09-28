package server

import (
	"context"
	"fmt"
	"github.com/viant/mcp-protocol/schema"
	protocol "github.com/viant/mcp-protocol/server"
	"testing"
)

type guardedCatalogService struct{ handlerService }

func (s *guardedCatalogService) AuthorizeCatalogTool(_ context.Context, name, action string) error {
	if name == "private" && action == "describe" {
		return fmt.Errorf("denied")
	}
	return nil
}
func TestListToolsRequiresDiscoveryAndDescriptionWithoutMutatingGeneration(t *testing.T) {
	registry := protocol.NewRegistry()
	for _, name := range []string{"public", "private"} {
		registry.RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: name}})
	}
	service := &guardedCatalogService{handlerService: handlerService{registry: registry}}
	factory, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.(*Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: "2025-11-25"}
	list, e := handler.ListTools(context.Background(), nil)
	if e != nil || len(list.Tools) != 1 || list.Tools[0].Name != "public" {
		t.Fatalf("list=%+v err=%v", list, e)
	}
	if len(registry.ListRegisteredTools()) != 2 {
		t.Fatal("request filtering mutated the published registry")
	}
}
