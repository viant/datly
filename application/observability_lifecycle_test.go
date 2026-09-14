package application_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/observability/otel"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type managedExport struct {
	mu     sync.Mutex
	spans  []sdktrace.ReadOnlySpan
	stops  atomic.Int32
	reads  atomic.Int32
	onStop func()
	closed chan struct{}
}

func (p *managedExport) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.spans = append(p.spans, spans...)
	return nil
}
func (p *managedExport) Shutdown(context.Context) error {
	p.stops.Add(1)
	if p.closed != nil {
		close(p.closed)
	}
	if p.onStop != nil {
		p.onStop()
	}
	return nil
}
func (p *managedExport) option() application.Option {
	return application.WithObservability(runtime.ObservabilityConfig{ReadingData: func(string, time.Duration, string, int, []any, error) { p.reads.Add(1) }, OTel: &otel.Config{Enabled: true, Exporter: p, QueueSize: 64, BatchSize: 16, BatchTimeout: time.Millisecond}})
}
func TestManagedObservabilityPinnedReloadAndDrainSQLite(t *testing.T) {
	f := &reloadFixture{block: true}
	f.init(t)
	p := &managedExport{closed: make(chan struct{})}
	manager, err := application.New(nil, p.option())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	ctx := context.Background()
	if err = manager.Reload(ctx, application.Request{Revision: 1, Compile: f.compile(1)}); err != nil {
		t.Fatal(err)
	}
	pinned, _, err := manager.Pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		res := httptest.NewRecorder()
		manager.ServeHTTP(res, httptest.NewRequest("GET", "/records", nil).WithContext(pinned))
		old <- res
	}()
	<-f.started
	if err = manager.Reload(ctx, application.Request{Revision: 2, Compile: f.compile(2)}); err != nil {
		t.Fatal(err)
	}
	current := httptest.NewRecorder()
	manager.ServeHTTP(current, httptest.NewRequest("GET", "/records", nil))
	if current.Code != 200 || !strings.Contains(current.Body.String(), "new") {
		t.Fatal(current.Body.String())
	}
	invalid := func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		built, err := f.compile(2)(ctx, types)
		if err != nil {
			return nil, err
		}
		age := int64(-1)
		built.HTTP.CORS = &spec.CORS{MaxAge: &age}
		return built, nil
	}
	if err = manager.Reload(ctx, application.Request{Revision: 3, Compile: invalid}); err == nil {
		t.Fatal("invalid HTTP stage published")
	}
	if manager.Revision() != 2 || p.stops.Load() != 0 {
		t.Fatal("failed/retired stage closed shared exporter")
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err = manager.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("must retain active pinned invocation: %v", err)
	}
	if p.stops.Load() != 0 {
		t.Fatal("exporter closed before pinned capture")
	}
	close(f.release)
	prior := <-old
	select {
	case <-p.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("deadline cleanup required a second Shutdown call")
	}
	if prior.Code != 200 || !strings.Contains(prior.Body.String(), "7") {
		t.Fatal(prior.Body.String())
	}
	if err = manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err = manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if p.stops.Load() != 1 {
		t.Fatalf("shutdown calls=%d", p.stops.Load())
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	sqlSpans := 0
	for _, span := range p.spans {
		if strings.HasPrefix(span.Name(), "SQL Select:") {
			sqlSpans++
		}
	}
	if p.reads.Load() != 3 {
		t.Fatalf("managed ReadingData calls=%d", p.reads.Load())
	}
	if sqlSpans != 3 {
		t.Fatalf("pinned nested and current reads lost: SQL spans=%d", sqlSpans)
	}
}

func TestManagedObservationWarmupShutdownOrderingSQLite(t *testing.T) {
	f := &httpReloadFixture{}
	f.init(t)
	entered := make(chan struct{}, 2)
	var completed atomic.Int32
	p := &managedExport{}
	p.onStop = func() {
		if completed.Load() != 2 {
			t.Errorf("exporter stopped before accepted warmups completed: %d", completed.Load())
		}
	}
	manager, err := application.New(nil, p.option())
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	config := httpReloadConfig()
	config.Warmup.Authorize = func(ctx context.Context, _ *http.Request, _ exec.ComponentTarget) error {
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	config.Warmup.Completed = func(gateway.WarmupResult, error) { completed.Add(1) }
	ctx := context.Background()
	if err = manager.Reload(ctx, application.Request{Revision: 1, Compile: f.compile(1, config, nil)}); err != nil {
		t.Fatal(err)
	}
	pinned, _, _ := manager.Pin(ctx)
	if err = manager.Reload(ctx, application.Request{Revision: 2, Compile: f.compile(2, config, nil)}); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 2)
	for _, request := range []struct {
		ctx context.Context
		key string
	}{{pinned, "1"}, {ctx, "2"}} {
		go func(request struct {
			ctx context.Context
			key string
		}) {
			req := httptest.NewRequest("POST", gateway.DefaultCacheWarmURI+"/records", nil).WithContext(request.ctx)
			req.Header.Set("X-Key", request.key)
			res := httptest.NewRecorder()
			manager.ServeHTTP(res, req)
			done <- res.Code
		}(request)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("authorization not entered")
		}
	}
	if err = manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if status := <-done; status == 200 {
			t.Fatal("cancelled warmup succeeded")
		}
	}
	if p.stops.Load() != 1 {
		t.Fatal("shared exporter not closed once")
	}
}

func TestManagedObservationRejectsStageOwnershipAndCleansFailedManager(t *testing.T) {
	f := &httpReloadFixture{}
	f.init(t)
	p := &managedExport{}
	manager, err := application.New(nil, p.option())
	if err != nil {
		t.Fatal(err)
	}
	stage := func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		built, err := f.compile(1, httpReloadConfig(), nil)(ctx, types)
		if err != nil {
			return nil, err
		}
		built.RuntimeOptions = append(built.RuntimeOptions, runtime.WithObservability(runtime.ObservabilityConfig{}))
		return built, nil
	}
	if err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: stage}); err == nil || !strings.Contains(err.Error(), "application owns observability") {
		t.Fatalf("stage ownership=%v", err)
	}
	if manager.Revision() != 0 || p.stops.Load() != 0 {
		t.Fatal("unpublished stage corrupted application lifetime")
	}
	if err = manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.stops.Load() != 1 {
		t.Fatal("failed application's exporter leaked")
	}
}
