package mcp

import (
	"context"
	"encoding/json"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

func (s *Service) SupportsResourceCatalog() bool {
	if s == nil {
		return false
	}
	if s.lazy != nil {
		if _, ok := s.lazy.targets["resources/list"]; ok {
			return true
		}
		return s.lazy.service().SupportsResourceCatalog()
	}
	_, ok := s.registry.ToolRegistry.Get("resources/list")
	return ok
}

// ListResourceCatalog invokes the application's existing authorized, paginated
// enumerator without fetching namespace collections or filtering their contents.
func (s *Service) ListResourceCatalog(ctx context.Context, request *schema.ListResourcesRequest) (*schema.ListResourcesResult, *jsonrpc.Error) {
	if request == nil {
		return nil, jsonrpc.NewInvalidParamsError("resource listing request required", nil)
	}
	if err := s.PrepareTool(ctx, "resources/list"); err != nil {
		return nil, jsonrpc.NewInternalError("prepare resource catalog", nil)
	}
	entry, ok := s.Registry().ToolRegistry.Get("resources/list")
	if !ok {
		return nil, jsonrpc.NewMethodNotFound("resource catalog unavailable", nil)
	}
	raw, err := json.Marshal(request.Params)
	if err != nil {
		return nil, jsonrpc.NewInvalidParamsError("invalid resource listing", nil)
	}
	arguments := map[string]interface{}{}
	if json.Unmarshal(raw, &arguments) != nil {
		return nil, jsonrpc.NewInvalidParamsError("invalid resource listing", nil)
	}
	result, rpc := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "resources/list", Arguments: arguments}})
	if rpc != nil {
		return nil, rpc
	}
	if result == nil {
		return nil, jsonrpc.NewInternalError("resource catalog returned no result", nil)
	}
	raw, err = json.Marshal(result.StructuredContent)
	if result.IsError != nil && *result.IsError {
		return nil, jsonrpc.NewInvalidParamsError("resource listing failed", raw)
	}
	if err != nil || string(raw) == "null" {
		return nil, jsonrpc.NewInternalError("invalid resource catalog response", nil)
	}
	var listed schema.ListResourcesResult
	if json.Unmarshal(raw, &listed) != nil || listed.Resources == nil {
		return nil, jsonrpc.NewInternalError("invalid resource catalog response", nil)
	}
	return &listed, nil
}
