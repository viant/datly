package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/viant/mcp-protocol/authorization"
	"net/http"
	"strings"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

const resourceFilterExtension = "io.viant/resources/filter"
const nativeResourceCursor = "datly-native:"
const catalogResourceCursor = "datly-catalog:"

type resourceCatalog interface {
	SupportsResourceCatalog() bool
	ListResourceCatalog(context.Context, *schema.ListResourcesRequest) (*schema.ListResourcesResult, *jsonrpc.Error)
}

func hasResourceFilter(request *schema.ListResourcesRequest) bool {
	raw, _ := json.Marshal(request.Params.Meta)
	meta := map[string]interface{}{}
	_ = json.Unmarshal(raw, &meta)
	_, ok := meta[resourceFilterExtension]
	return ok
}

// listResourceCatalog preserves native mapped inventory pages before the
// application's inventory. Scoped requests go directly to its SQL-backed page.
func (h *Handler) listResourceCatalog(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ListResourcesRequest]) (*schema.ListResourcesResult, *jsonrpc.Error, bool) {
	catalog, ok := h.service.(resourceCatalog)
	if !ok || !catalog.SupportsResourceCatalog() {
		return nil, nil, false
	}
	if request == nil || request.Request == nil {
		return nil, jsonrpc.NewInvalidParamsError("resource listing request required", nil), true
	}
	input := *request.Request
	scoped := hasResourceFilter(&input)
	cursor := ""
	if input.Params.Cursor != nil {
		cursor = *input.Params.Cursor
	}
	if scoped {
		returned, err := catalog.ListResourceCatalog(ctx, &input)
		return returned, err, true
	}
	if strings.HasPrefix(cursor, catalogResourceCursor) {
		token := strings.TrimPrefix(cursor, catalogResourceCursor)
		input.Params.Cursor = nil
		if token != "" {
			raw, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				return nil, jsonrpc.NewInvalidParamsError("invalid resource cursor", nil), true
			}
			value := string(raw)
			input.Params.Cursor = &value
		}
		result, rpc := catalog.ListResourceCatalog(ctx, &input)
		if result != nil && result.NextCursor != nil {
			next := catalogResourceCursor + base64.RawURLEncoding.EncodeToString([]byte(*result.NextCursor))
			result.NextCursor = &next
		}
		return result, rpc, true
	}
	nativeRequest := *request
	nativeRequest.Request = &input
	if strings.HasPrefix(cursor, nativeResourceCursor) {
		raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(cursor, nativeResourceCursor))
		if err != nil {
			return nil, jsonrpc.NewInvalidParamsError("invalid resource cursor", nil), true
		}
		value := string(raw)
		input.Params.Cursor = &value
	}
	result, rpc := h.DefaultHandler.ListResources(ctx, &nativeRequest)
	if rpc != nil {
		return nil, rpc, true
	}
	if result.NextCursor != nil {
		next := nativeResourceCursor + base64.RawURLEncoding.EncodeToString([]byte(*result.NextCursor))
		result.NextCursor = &next
		return result, nil, true
	}
	next := catalogResourceCursor
	if len(result.Resources) > 0 {
		result.NextCursor = &next
		return result, nil, true
	}
	input.Params.Cursor = &next
	nativeRequest.Request = &input
	return h.listResourceCatalog(ctx, &nativeRequest)
}

// Forward only the presented credential. The compiled catalog component owns
// JWT verification and resource policy, exactly as it does for tools/call.
func forwardCatalogBearer(service Service, next http.Handler) http.Handler {
	catalog, ok := service.(resourceCatalog)
	if !ok || !catalog.SupportsResourceCatalog() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Context().Value(authorization.TokenKey) == nil {
			bearer := strings.TrimSpace(r.Header.Get("Authorization"))
			if strings.HasPrefix(strings.ToLower(bearer), "bearer ") {
				r = r.WithContext(context.WithValue(r.Context(), authorization.TokenKey, &authorization.Token{Token: bearer}))
			}
		}
		next.ServeHTTP(w, r)
	})
}

func publishResourceFilterCapability(service Service, capabilities *schema.ServerCapabilities) {
	if target, ok := service.(interface{ SupportsWindowTarget() bool }); ok && target.SupportsWindowTarget() {
		if capabilities.Extensions == nil {
			capabilities.Extensions = map[string]map[string]interface{}{}
		}
		capabilities.Extensions["io.viant/window/target"] = map[string]interface{}{"schemaVersion": 1, "fields": []string{"platform", "formFactor", "surface", "capabilities"}}
	}
	if catalog, ok := service.(resourceCatalog); ok && catalog.SupportsResourceCatalog() {
		if capabilities.Extensions == nil {
			capabilities.Extensions = map[string]map[string]interface{}{}
		}
		capabilities.Extensions[resourceFilterExtension] = map[string]interface{}{"schemaVersion": 1, "fields": []string{"namespace", "kind", "query", "limit"}, "pagination": "cursor"}
	}
}
