package mcp

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/client"
	xmcp "github.com/viant/xdatly/client/mcp"
)

func timeAt(second int) time.Time {
	return time.Date(2026, 9, 23, 12, 0, second, 0, time.UTC)
}

func TestSessionKeyPreservesHeaderValues(t *testing.T) {
	first := http.Header{"X-Gateway": {"first", "second"}, "Authorization": {"Bearer alice"}}
	same := http.Header{"Authorization": {"Bearer alice"}, "X-Gateway": {"first", "second"}}
	if SessionKey(first) != SessionKey(same) {
		t.Fatal("map insertion order changed session identity")
	}
	for _, other := range []http.Header{
		{"X-Gateway": {"second", "first"}, "Authorization": {"Bearer alice"}},
		{"X-Gateway": {"first", "second"}, "Authorization": {"Bearer bob"}},
		{"X-Gateway": {"first\x00second"}, "Authorization": {"Bearer alice"}},
	} {
		if SessionKey(first) == SessionKey(other) {
			t.Fatal("different outbound headers share a session key")
		}
	}
}

func TestClientIsolatesSessionsByExactHeaderSetAndEvictsOldest(t *testing.T) {
	c, err := New(xmcp.Options{URL: "http://127.0.0.1:9/mcp", Tool: "ctx", Header: map[string]string{"X-Static": "s"}, MaxSessions: 2, Limits: client.Limits{Timeout: "1s"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	base := c.sessionHeader(http.Header{"Authorization": {"Bearer alice"}})
	if base.Get("X-Static") != "s" || base.Get("Authorization") != "Bearer alice" {
		t.Fatalf("merged header = %v", base)
	}
	overridden := c.sessionHeader(http.Header{"X-Static": {"explicit"}})
	if overridden.Get("X-Static") != "explicit" {
		t.Fatalf("explicit header did not replace static: %v", overridden)
	}
	first, err := c.session(base, timeAt(1))
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.session(base, timeAt(2))
	if err != nil || again != first {
		t.Fatal("identical header set did not reuse the session")
	}
	if _, err := c.session(c.sessionHeader(http.Header{"Authorization": {"Bearer bob"}}), timeAt(3)); err != nil {
		t.Fatal(err)
	}
	if c.Sessions() != 2 {
		t.Fatalf("sessions = %d", c.Sessions())
	}
	if _, err := c.session(c.sessionHeader(http.Header{"Authorization": {"Bearer carol"}}), timeAt(4)); err != nil {
		t.Fatal(err)
	}
	if c.Sessions() != 2 {
		t.Fatalf("maxSessions not enforced: %d", c.Sessions())
	}
	if _, ok := c.sessions[SessionKey(base)]; ok {
		t.Fatal("least recently used session survived eviction")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.session(base, timeAt(5)); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("closed client opened a session: %v", err)
	}
	if _, err := c.CallTool(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "other"}}, xmcp.CallOptions{}); err == nil || !strings.Contains(err.Error(), "does not match configured tool") {
		t.Fatalf("tool mismatch accepted: %v", err)
	}
	if _, err := c.CallTool(context.Background(), nil, xmcp.CallOptions{}); err == nil {
		t.Fatal("nil request accepted")
	}
}
