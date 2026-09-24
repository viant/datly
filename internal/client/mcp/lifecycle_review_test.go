package mcp

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xmcp "github.com/viant/xdatly/client/mcp"
)

func TestEvictedSessionCannotReconnect(t *testing.T) {
	var attempts atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	c, err := New(xmcp.Options{URL: server.URL, Tool: "context", MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	alice, err := c.session(http.Header{"Authorization": {"alice"}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.session(http.Header{"Authorization": {"bob"}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// An invocation may retain this pointer across eviction. It must not open
	// a connection no longer owned by the pool, which Close could never release.
	if _, err := c.connect(context.Background(), alice); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("evicted session was not rejected: %v", err)
	}
	if attempts.Load() != 0 {
		t.Fatal("evicted session opened an unowned connection")
	}
}

func TestZeroSessionLimitIsUnlimited(t *testing.T) {
	c, err := New(xmcp.Options{URL: "http://127.0.0.1:9", Tool: "context"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := range 100 {
		if _, err := c.session(http.Header{"Authorization": {fmt.Sprint(i)}}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if c.Sessions() != 100 {
		t.Fatalf("zero session limit imposed a hidden cap: %d", c.Sessions())
	}
}
