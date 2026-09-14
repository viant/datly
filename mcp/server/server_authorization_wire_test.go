package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
)

func TestHTTPResourceAuthorizationAndMetadataThroughLocalWrapper(t *testing.T) {
	service := newTransportTestService(testAuthorizationPolicy())
	wire := startWireServer(t, service)
	sessionID := initializeWireSession(t, wire.baseURL)
	payload := map[string]interface{}{
		"jsonrpc": "2.0", "id": 2, "method": schema.MethodResourcesRead,
		"params": map[string]interface{}{"uri": testResourceURI},
	}
	_, header, status := postWireRequest(t, wire.baseURL+"/mcp", payload, sessionID, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d headers=%v", status, header)
	}
	metadataURL := challengeMetadataURL(t, header.Get("WWW-Authenticate"))
	if !strings.Contains(header.Get("WWW-Authenticate"), `, scope="read"`) {
		t.Fatalf("challenge=%q", header.Get("WWW-Authenticate"))
	}
	assertProtectedResourceMetadata(t, metadataURL)

	response, _, status := postWireRequest(t, wire.baseURL+"/mcp", payload, sessionID, "Bearer secret")
	if status != http.StatusOK || response.Error != nil {
		t.Fatalf("authorized status=%d error=%+v result=%s", status, response.Error, response.Result)
	}
	if service.observedToken() != "Bearer secret" {
		t.Fatalf("protocol token=%q", service.observedToken())
	}
	result := &schema.ReadResourceResult{}
	if err := json.Unmarshal(response.Result, result); err != nil {
		t.Fatal(err)
	}
	if len(result.Contents) != 1 || result.Contents[0].Text != "ready" {
		t.Fatalf("resource result=%+v", result)
	}

	unknown := wire.baseURL + "/.well-known/oauth-protected-resource?resource=" + url.QueryEscape("datly://localhost/missing")
	metadataResponse, err := http.Get(unknown)
	if err != nil {
		t.Fatal(err)
	}
	defer metadataResponse.Body.Close()
	if metadataResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown metadata status=%d", metadataResponse.StatusCode)
	}
}

func challengeMetadataURL(t *testing.T, challenge string) string {
	t.Helper()
	const prefix = `Bearer resource_metadata="`
	if !strings.HasPrefix(challenge, prefix) {
		t.Fatalf("challenge=%q", challenge)
	}
	value := strings.TrimPrefix(challenge, prefix)
	end := strings.IndexByte(value, '"')
	if end < 0 {
		t.Fatalf("challenge=%q", challenge)
	}
	return value[:end]
}

func assertProtectedResourceMetadata(t *testing.T, endpoint string) {
	t.Helper()
	response, err := http.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	metadata := &oauthmeta.ProtectedResourceMetadata{}
	if response.StatusCode != http.StatusOK || json.Unmarshal(body, metadata) != nil || metadata.Resource != "https://api.example.com" {
		t.Fatalf("metadata status=%d body=%s", response.StatusCode, body)
	}
}
