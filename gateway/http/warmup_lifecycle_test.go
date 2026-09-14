package http

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	dsql "github.com/viant/datly/sql"
)

type warmupGate struct {
	after error
	dexec.Reader
	dexec.QueryPreparer
	warmer  dexec.ReaderWarmer
	started chan context.Context
	release chan struct{}
}

func (g *warmupGate) Warmup(ctx context.Context, request dexec.ReaderWarmupInvocation) (int, error) {
	select {
	case g.started <- ctx:
	default:
	}
	select {
	case <-g.release:
		count, err := g.warmer.Warmup(ctx, request)
		if err == nil {
			err = g.after
		}
		return count, err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (f *configFixture) gated(t *testing.T) (*druntime.Runtime, *warmupGate) {
	t.Helper()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: f.component, InputType: reflect.TypeFor[configInput](), OutputType: reflect.TypeFor[configOutput](), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	gate := &warmupGate{Reader: reader, QueryPreparer: reader.(dexec.QueryPreparer), warmer: reader.(dexec.ReaderWarmer), started: make(chan context.Context, 1), release: make(chan struct{})}
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[configOutput](), Reader: gate}})
	if err != nil {
		t.Fatal(err)
	}
	return rt, gate
}

func TestHTTPWarmupServerLifetimeSQLite(t *testing.T) {
	type secretKey struct{}
	for _, mode := range []string{"client cancellation", "shutdown", "operation timeout", "server cancellation"} {
		t.Run(mode, func(t *testing.T) {
			f := newConfigFixture(t)
			rt, gate := f.gated(t)
			config := warmupConfig()
			serverCtx, stopServer := context.WithCancel(context.Background())
			defer stopServer()
			config.Warmup.Lifetime = NewWarmupLifetime(serverCtx)
			if mode == "operation timeout" {
				config.Warmup.Timeout = 100 * time.Millisecond
			}
			h, err := config.NewHandler(rt, nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			defer h.Shutdown(context.Background())
			caller, cancel := context.WithCancel(context.WithValue(context.Background(), secretKey{}, "private caller state"))
			defer cancel()
			req := httptest.NewRequest("POST", "/admin/warm/records", nil).WithContext(caller)
			req.Header.Set("X-Admin", "admin")
			res := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { defer close(done); h.ServeHTTP(res, req) }()
			var operation context.Context
			select {
			case operation = <-gate.started:
			case <-time.After(5 * time.Second):
				t.Fatal("warmup did not start")
			}
			if operation.Value(secretKey{}) != nil {
				t.Fatal("retained caller context")
			}
			switch mode {
			case "client cancellation":
				cancel()
				if operation.Err() != nil {
					t.Fatal(operation.Err())
				}
				close(gate.release)
			case "shutdown":
				if err := h.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "server cancellation":
				stopServer()
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("warmup did not finish")
			}
			if mode != "client cancellation" {
				if (res.Code != 503 && res.Code != 504) || !errors.Is(operation.Err(), context.Canceled) && !errors.Is(operation.Err(), context.DeadlineExceeded) {
					t.Fatalf("status=%d error=%v", res.Code, operation.Err())
				}
				return
			}
			if res.Code != 200 {
				t.Fatalf("%d %s", res.Code, res.Body.String())
			}
			if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			cached := httptest.NewRecorder()
			h.ServeHTTP(cached, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
			if cached.Code != 200 {
				t.Fatalf("canceled caller lost warmup cache: %s", cached.Body.String())
			}
		})
	}
}

var _ dexec.ReaderWarmer = (*warmupGate)(nil)
