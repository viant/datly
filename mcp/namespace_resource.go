package mcp

import (
	"context"
	"encoding/json"

	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

// readNamespaceResource adapts the conventional namespace-extension resolver.
// Only an unmapped URI reaches this bridge. Its compiled tool handler retains
// the exact existing argument binding, component invocation and authorization.
func (s *Service) readNamespaceResource(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error, bool) {
	entry, ok := s.registry.ToolRegistry.Get("resource/get")
	if !ok {
		return nil, nil, false
	}
	result, protocolErr := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "resource/get", Arguments: map[string]interface{}{"uri": request.Params.Uri}}})
	if protocolErr != nil {
		return nil, protocolErr, true
	}
	if result == nil {
		return nil, jsonrpc.NewInternalError("resource resolver returned no result", nil), true
	}
	if result.IsError != nil && *result.IsError {
		payload, _ := json.Marshal(result.StructuredContent)
		var failure struct {
			Status int             `json:"status"`
			Code   json.RawMessage `json:"code"`
		}
		_ = json.Unmarshal(payload, &failure)
		if failure.Status == 0 {
			_ = json.Unmarshal(failure.Code, &failure.Status)
		}
		switch failure.Status {
		case 404:
			return nil, schema.NewResourceNotFound(request.Params.Uri), true
		case 401, 403:
			return nil, jsonrpc.NewInvalidRequest("resource authorization denied", payload), true
		}
		if failure.Status >= 500 {
			return nil, jsonrpc.NewInternalError("resource resolution failed", payload), true
		}
		return nil, jsonrpc.NewInvalidParamsError("resource resolution failed", payload), true
	}
	var payload []byte
	var err error
	if result.StructuredContent != nil {
		payload, err = json.Marshal(result.StructuredContent)
	} else {
		for _, content := range result.Content {
			if value, ok := content.(schema.TextContent); ok && json.Valid([]byte(value.Text)) {
				payload = []byte(value.Text)
				break
			}
		}
	}
	if err != nil || len(payload) == 0 {
		return nil, jsonrpc.NewInternalError("resource resolver returned invalid JSON", nil), true
	}
	mime := "application/json"
	return &schema.ReadResourceResult{ResultType: schema.ResultTypeComplete, CacheScope: schema.ReadResourceResultCacheScopePrivate, TtlMs: 0, Contents: []schema.ReadResourceResultContentsElem{{Uri: request.Params.Uri, MimeType: &mime, Text: string(payload)}}}, nil, true
}

func hasNamespaceResourceResolver(catalog *Catalog, indexed []*spec.Component) bool {
	if _, ok := catalog.Tool("resource/get"); ok {
		return true
	}
	for _, component := range indexed {
		if component == nil {
			continue
		}
		for _, route := range component.Routes {
			if route == nil {
				continue
			}
			for _, exposure := range route.MCP {
				if exposure != nil && exposure.Kind == spec.MCPExposureTool && exposure.Identity(component, route) == "resource/get" {
					return true
				}
			}
		}
	}
	return false
}
