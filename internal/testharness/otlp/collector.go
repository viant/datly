// Package otlp provides a real protobuf HTTP collector for integration tests.
package otlp

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type Collector struct {
	Server   *httptest.Server
	mu       sync.Mutex
	requests []*collector.ExportTraceServiceRequest
	keys     []string
	gate     <-chan struct{}
	Started  chan struct{}
	once     sync.Once
}

func New(t *testing.T, gate <-chan struct{}) *Collector {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("OTLP integration requires local TCP: %v", err)
	}
	c := &Collector{gate: gate, Started: make(chan struct{})}
	c.Server = &httptest.Server{Listener: listener, Config: &http.Server{Handler: c}}
	c.Server.Start()
	t.Cleanup(c.Server.Close)
	return c
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/traces" || r.Method != "POST" {
		http.NotFound(w, r)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8<<20))
	if err != nil {
		http.Error(w, "body", 400)
		return
	}
	request := &collector.ExportTraceServiceRequest{}
	if err = proto.Unmarshal(data, request); err != nil {
		http.Error(w, "protobuf", 400)
		return
	}
	c.mu.Lock()
	c.requests = append(c.requests, request)
	c.keys = append(c.keys, r.Header.Get("Authorization"))
	c.mu.Unlock()
	c.once.Do(func() { close(c.Started) })
	if c.gate != nil {
		select {
		case <-c.gate:
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(200)
}

func (c *Collector) Snapshot() ([]*collector.ExportTraceServiceRequest, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*collector.ExportTraceServiceRequest, len(c.requests))
	for i, r := range c.requests {
		result[i] = proto.Clone(r).(*collector.ExportTraceServiceRequest)
	}
	return result, append([]string(nil), c.keys...)
}

func (c *Collector) Wait(ctx context.Context, count int) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		n := len(c.requests)
		c.mu.Unlock()
		if n >= count {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
