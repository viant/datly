package application_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/typecatalog"
)

func TestManagerWarmupAcceptedPreparationLifetimeSQLite(t *testing.T) {
	type callerSecret struct{}
	for _, phase := range []string{"authorization", "preparation"} {
		for _, action := range []string{"client cancellation", "timeout", "shutdown", "denied"} {
			if phase == "preparation" && action == "denied" {
				continue
			}
			t.Run(phase+"/"+action, func(t *testing.T) {
				f := &httpReloadFixture{}
				f.init(t)
				manager, err := application.New(nil)
				if err != nil {
					t.Fatal(err)
				}
				defer manager.Shutdown(context.Background())
				entered := make(chan context.Context, 1)
				release := make(chan struct{})
				gate := &httpPreparationGate{}
				config := httpReloadConfig()
				baseAuthorize := config.Warmup.Authorize
				if action == "timeout" {
					config.Warmup.Timeout = 60 * time.Millisecond
				}
				config.Warmup.Authorize = func(ctx context.Context, req *http.Request, target exec.ComponentTarget) error {
					if ctx.Value(callerSecret{}) != nil || req.Context() != ctx || req.Header.Get("X-Unrelated") != "" || req.URL.RawQuery != "" || req.URL.User != nil || req.TLS != nil || req.Body != http.NoBody || req.GetBody != nil {
						return fmt.Errorf("unsafe request snapshot")
					}
					if phase == "authorization" {
						entered <- ctx
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					if action == "denied" && phase == "authorization" {
						return fmt.Errorf("administrator denied")
					}
					return baseAuthorize(ctx, req, target)
				}
				if phase == "preparation" {
					gate.started = entered
					gate.release = release
				}
				compile := f.compile(1, config, gate)
				if err := manager.Reload(context.Background(), application.Request{Revision: 1, Compile: compile}); err != nil {
					t.Fatal(err)
				}
				caller, cancel := context.WithCancel(context.WithValue(context.Background(), callerSecret{}, "private"))
				defer cancel()
				req := httptest.NewRequest("POST", gateway.DefaultCacheWarmURI+"/records?target=untrusted", nil).WithContext(caller)
				req.Header.Set("X-Key", "1")
				req.Header.Set("X-Admin", "admin")
				req.Header.Set("X-Unrelated", "must not retain")
				res := httptest.NewRecorder()
				done := make(chan struct{})
				go func() { defer close(done); manager.ServeHTTP(res, req) }()
				var operation context.Context
				select {
				case operation = <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("accepted operation did not enter slow phase")
				}
				switch action {
				case "client cancellation":
					cancel()
					req.Header.Set("X-Admin", "changed")
					req.URL.RawQuery = "changed"
					close(release)
				case "shutdown":
					if err := manager.Shutdown(context.Background()); err != nil {
						t.Fatal(err)
					}
				case "denied":
					if phase == "preparation" { // Server policy cancellation at the canonical preparation boundary.
						if err := manager.Shutdown(context.Background()); err != nil {
							t.Fatal(err)
						}
					} else {
						close(release)
					}
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("accepted operation orphaned")
				}
				if action == "client cancellation" {
					if res.Code != 200 || gate.calls.Load() != 1 {
						t.Fatalf("client canceled valid %s: %d %s calls=%d", phase, res.Code, res.Body.String(), gate.calls.Load())
					}
					if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
					read := httptest.NewRequest("GET", "/records?tenant=1", nil)
					read.Header.Set("X-Key", "1")
					cached := httptest.NewRecorder()
					manager.ServeHTTP(cached, read)
					if cached.Code != 200 {
						t.Fatalf("cache not reusable: %s", cached.Body.String())
					}
					return
				}
				want := 503
				if action == "timeout" {
					want = 504
				}
				if action == "denied" && phase == "authorization" {
					want = 403
				}
				if res.Code != want || gate.calls.Load() != 0 {
					t.Fatalf("denied operation warmed: %d want=%d calls=%d ctx=%v", res.Code, want, gate.calls.Load(), operation.Err())
				}
			})
		}
	}
}

func TestManagerShutdownJoinsPinnedAndCurrentPreparationSQLite(t *testing.T) {
	f := &httpReloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	oldGate := &httpPreparationGate{started: make(chan context.Context, 1), release: make(chan struct{})}
	if err := manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile(1, httpReloadConfig(), oldGate)}); err != nil {
		t.Fatal(err)
	}
	pinned, _, err := manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	newGate := &httpPreparationGate{started: make(chan context.Context, 1), release: make(chan struct{})}
	if err := manager.Reload(context.Background(), application.Request{Revision: 2, Compile: f.compile(2, httpReloadConfig(), newGate)}); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 2)
	for _, tc := range []struct {
		ctx context.Context
		key string
	}{{pinned, "1"}, {context.Background(), "2"}} {
		req := httptest.NewRequest("POST", gateway.DefaultCacheWarmURI+"/records", nil).WithContext(tc.ctx)
		req.Header.Set("X-Key", tc.key)
		req.Header.Set("X-Admin", "admin")
		go func(req *http.Request) { res := httptest.NewRecorder(); manager.ServeHTTP(res, req); done <- res.Code }(req)
	}
	for _, gate := range []*httpPreparationGate{oldGate, newGate} {
		select {
		case <-gate.started:
		case <-time.After(5 * time.Second):
			t.Fatal("pinned/current preparation did not enter")
		}
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case status := <-done:
			if status != 503 {
				t.Fatalf("shutdown status=%d", status)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("shutdown orphaned generation")
		}
	}
	if oldGate.calls.Load() != 0 || newGate.calls.Load() != 0 {
		t.Fatal("shutdown proceeded to cache dispatch")
	}
}

func TestManagerShutdownDeadlineRetainsAcceptedAuthorization(t *testing.T) {
	f := &httpReloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	gate := &httpPreparationGate{}
	config := httpReloadConfig()
	config.Warmup.Authorize = func(context.Context, *http.Request, exec.ComponentTarget) error {
		close(entered)
		<-release
		return nil
	}
	if err := manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile(1, config, gate)}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", gateway.DefaultCacheWarmURI+"/records", nil)
	req.Header.Set("X-Key", "1")
	res := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); manager.ServeHTTP(res, req) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("authorization not entered")
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := manager.Shutdown(shutdown); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown did not report unfinished work: %v", err)
	}
	select {
	case <-done:
		t.Fatal("authorization unexpectedly ended")
	default:
	}
	close(release)
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-done
	if res.Code != 503 || gate.calls.Load() != 0 {
		t.Fatalf("late authorization warmed after shutdown: %d calls=%d", res.Code, gate.calls.Load())
	}
}

func TestManagerShutdownRejectsInFlightPublication(t *testing.T) {
	f := &httpReloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- manager.Reload(context.Background(), application.Request{Revision: 1, Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(1, httpReloadConfig(), nil)(ctx, types)
			close(entered)
			<-release
			return built, err
		}})
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("stage did not enter")
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, application.ErrClosed) {
		t.Fatalf("late stage=%v", err)
	}
	if manager.Revision() != 0 {
		t.Fatal("shutdown published a new generation")
	}
}
