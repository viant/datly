package server

import (
	"context"
	"testing"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

type listingCatalogService struct {
	*transportTestService
	calls int
	last  *schema.ListResourcesRequest
}

func (s *listingCatalogService) SupportsResourceCatalog() bool { return true }
func (s *listingCatalogService) ListResourceCatalog(_ context.Context, request *schema.ListResourcesRequest) (*schema.ListResourcesResult, *jsonrpc.Error) {
	s.calls++
	s.last = request
	result := &schema.ListResourcesResult{Resources: []schema.Resource{{Name: "dynamic", Uri: "navigation://app/main"}}}
	if request.Params.Cursor == nil {
		cursor := "provider-page-2"
		result.NextCursor = &cursor
	}
	return result, nil
}
func TestResourceCatalogPreservesNativeInventoryAndOpaqueProviderPages(t *testing.T) {
	service := &listingCatalogService{transportTestService: newTransportTestService(nil)}
	factory, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := raw.(*Handler)
	request := &jsonrpc.TypedRequest[*schema.ListResourcesRequest]{Request: &schema.ListResourcesRequest{}}
	first, rpc := handler.ListResources(context.Background(), request)
	if rpc != nil || len(first.Resources) != 1 || first.Resources[0].Uri != testResourceURI || first.NextCursor == nil || service.calls != 0 {
		t.Fatalf("native inventory lost: %+v %v", first, rpc)
	}
	request.Request.Params.Cursor = first.NextCursor
	second, rpc := handler.ListResources(context.Background(), request)
	if rpc != nil || len(second.Resources) != 1 || second.Resources[0].Uri != "navigation://app/main" || second.NextCursor == nil || service.calls != 1 || service.last.Params.Cursor != nil {
		t.Fatalf("catalog transition failed: %+v %v", second, rpc)
	}
	request.Request.Params.Cursor = second.NextCursor
	third, rpc := handler.ListResources(context.Background(), request)
	if rpc != nil || third.NextCursor != nil || service.last.Params.Cursor == nil || *service.last.Params.Cursor != "provider-page-2" {
		t.Fatalf("provider cursor changed: %+v %v", third, rpc)
	}
}
func TestResourceCatalogFilterSkipsNativeInventoryAndAdvertisesCapability(t *testing.T) {
	service := &listingCatalogService{transportTestService: newTransportTestService(nil)}
	factory, _ := NewHandler(service)
	raw, _ := factory(context.Background(), nil, nil, nil)
	handler := raw.(*Handler)
	request := &schema.ListResourcesRequest{}
	request.Params.Meta.AdditionalProperties = map[string]interface{}{resourceFilterExtension: map[string]interface{}{"namespace": "ns://app", "kind": "navigation"}}
	result, rpc := handler.ListResources(context.Background(), &jsonrpc.TypedRequest[*schema.ListResourcesRequest]{Request: request})
	if rpc != nil || len(result.Resources) != 1 || result.Resources[0].Uri == testResourceURI || service.calls != 1 {
		t.Fatalf("scoped inventory mixed native records: %+v %v", result, rpc)
	}
	var initialized schema.DiscoverResult
	handler.Discover(context.Background(), &initialized)
	if initialized.Capabilities.Extensions[resourceFilterExtension]["pagination"] != "cursor" {
		t.Fatal("filter capability not advertised")
	}
}
