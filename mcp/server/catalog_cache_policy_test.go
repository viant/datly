package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/mcp-protocol/schema"
	protocol "github.com/viant/mcp-protocol/server"
)

func TestLatestToolCatalogWireDeclaresPrivateLease(t *testing.T) {
	zero, custom := int64(0), int64(1234)
	for _, tc := range []struct {
		name string
		ttl  *int64
		want int
	}{{"default", nil, 300000}, {"explicit zero", &zero, 0}, {"configured", &custom, 1234}} {
		t.Run(tc.name, func(t *testing.T) {
			wire := startWireServerConfig(t, Config{Service: newTransportTestService(nil), ToolsListTtlMs: tc.ttl, Transport: TransportConfig{Kind: TransportStreamable}})
			payload := map[string]any{"jsonrpc": "2.0", "id": 2, "method": schema.MethodToolsList, "params": map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": schema.LatestProtocolVersion, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}}
			response, _, status := postWireRequest(t, wire.baseURL+"/mcp", payload, "", "")
			require.Equal(t, http.StatusOK, status)
			require.Nil(t, response.Error)
			var result schema.ListToolsResult
			require.NoError(t, json.Unmarshal(response.Result, &result))
			require.Equal(t, schema.ListToolsResultCacheScopePrivate, result.CacheScope)
			require.Equal(t, tc.want, result.TtlMs)
			require.Len(t, result.Tools, 1)
		})
	}
}

func TestPrivateCatalogLeaseRetainsServerAuthorizationFiltering(t *testing.T) {
	registry := protocol.NewRegistry()
	for _, name := range []string{"public", "private"} {
		registry.RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: name}})
	}
	service := &guardedCatalogService{handlerService: handlerService{registry: registry}}
	factory, err := NewHandler(service)
	require.NoError(t, err)
	handler, err := factory(context.Background(), nil, nil, nil)
	require.NoError(t, err)
	handler.(*Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: schema.LatestProtocolVersion}
	result, rpcErr := handler.ListTools(context.Background(), nil)
	require.Nil(t, rpcErr)
	require.Equal(t, schema.ListToolsResultCacheScopePrivate, result.CacheScope)
	require.EqualValues(t, DefaultToolsListTtlMs, result.TtlMs)
	require.Len(t, result.Tools, 1)
	require.Equal(t, "public", result.Tools[0].Name)
	require.Len(t, registry.ListRegisteredTools(), 2)
}

func TestCatalogLeaseRejectsInvalidConfiguration(t *testing.T) {
	for _, ttl := range []int64{-1, 1<<63 - 1} {
		_, err := New(Config{Service: newTransportTestService(nil), ToolsListTtlMs: &ttl})
		require.Error(t, err)
	}
}
