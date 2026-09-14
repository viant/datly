package standalone

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/otlp"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestConfiguredExporterPinnedGenerationDrain(t *testing.T) {
	release := make(chan struct{})
	collector := otlp.New(t, release)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	f := fixture.New(t)
	f.Services(t, collector.Server.URL)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	// Hold export across source recompilation; shutdown is the deadline under test.
	cfg.Observation.OTel.ExportTimeoutMs = int64((5 * time.Minute) / time.Millisecond)
	cfg.Observation.OTel.QueueSize = 1
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); _ = s.Shutdown(context.Background()) }()
	if err = s.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	pinned, _, err := s.manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/records/1", nil).WithContext(pinned)
	req.Header.Set("X-Read", "read-key")
	s.manager.ServeHTTP(res, req)
	if res.Code != 200 {
		t.Fatal(res.Body.String())
	}
	select {
	case <-collector.Started:
	case <-time.After(3 * time.Second):
		t.Fatal("configured exporter not called")
	}
	// The request already returned while the real exporter response is blocked.
	if err = s.Reload(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	age := int64(-1)
	s.source.http.CORS = &spec.CORS{MaxAge: &age}
	if err = s.Reload(context.Background(), 3); err == nil || s.manager.Revision() != 2 {
		t.Fatal("invalid reload published")
	}
	res = httptest.NewRecorder()
	s.manager.ServeHTTP(res, req)
	if res.Code != 200 {
		t.Fatal("pinned generation lost")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain %v", err)
	}
	if err = s.source.connections.SQL.DB.Ping(); err != nil {
		t.Fatal("database closed before export drained")
	}
	unblock()
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.source.connections.SQL.DB.Ping(); err == nil {
		t.Fatal("owned database remained open")
	}
	if requests, _ := collector.Snapshot(); len(requests) != 1 {
		t.Fatalf("bounded queue should export one accepted invocation, got %d", len(requests))
	}
}
