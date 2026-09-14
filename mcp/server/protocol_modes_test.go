package server

import (
	"encoding/json"
	"github.com/viant/mcp-protocol/schema"
	"net/http"
	"testing"
)

func TestHTTPToolListingAcrossProtocolModes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		session bool
	}{{"session negotiation", true}, {"stateless metadata", false}} {
		t.Run(tc.name, func(t *testing.T) {
			server := startWireServer(t, newTransportTestService(nil))
			session := ""
			params := map[string]interface{}{}
			if tc.session {
				session = initializeWireSession(t, server.baseURL)
			} else {
				params["_meta"] = map[string]interface{}{"io.modelcontextprotocol/protocolVersion": schema.LatestProtocolVersion, "io.modelcontextprotocol/clientCapabilities": map[string]interface{}{}}
			}
			payload := map[string]interface{}{"jsonrpc": "2.0", "id": 2, "method": schema.MethodToolsList, "params": params}
			response, _, status := postWireRequest(t, server.baseURL+"/mcp", payload, session, "")
			if status != http.StatusOK || response.Error != nil {
				t.Fatalf("status=%d error=%v result=%s", status, response.Error, response.Result)
			}
			var result schema.ListToolsResult
			if err := json.Unmarshal(response.Result, &result); err != nil {
				t.Fatal(err)
			}
			if len(result.Tools) != 1 || result.Tools[0].Name != "registered" {
				t.Fatalf("tools=%+v", result.Tools)
			}
		})
	}
}
