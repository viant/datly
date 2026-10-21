package gateway

import "testing"

func TestModelContextProtocolProtectedResourceURL(t *testing.T) {
	const configured = "https://steward.example/mcp"
	if got := (&ModelContextProtocol{ResourceURL: configured}).ProtectedResourceURL(); got != configured {
		t.Fatalf("protected resource URL = %q, want %q", got, configured)
	}
	if got := (&ModelContextProtocol{}).ProtectedResourceURL(); got != defaultMCPProtectedResource {
		t.Fatalf("default protected resource URL = %q, want %q", got, defaultMCPProtectedResource)
	}
}
