// Package standalone composes source-backed applications and owns their listeners.
package standalone

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/internal/httpserver"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/mcp/server/auth"
	"github.com/viant/x"
	xmodule "github.com/viant/x/module"
	xcodec "github.com/viant/xdatly/codec"
)

type Options struct {
	// Codecs supplies named application codec factories; built-in names are reserved.
	Codecs map[string]xcodec.Factory
	// Async supplies trusted authorization and optional AFS callbacks for Config.Jobs.
	Async     *AsyncOptions
	Workspace *xmodule.Workspace
	// Holders are concrete component declarations emitted into a custom build.
	// Runtime package names still come exclusively from Config.GoBootstrap.
	Holders []any
	// RequireLinked rejects Go holder source that is absent from the executable's
	// runtime typelinks. Custom commands enable it; direct authoring tests may not.
	RequireLinked bool
	// MCPResourceAuthorizer uses the linked deployment's existing token/scope
	// verifier for protected embedded resources. Nil denies protected skill reads.
	MCPResourceAuthorizer auth.ResourceAuthorizer
	// Config is transferred to the server; callers must not mutate it after New.
	Config *config.Config
	// Registry supplies compiled package exports, including typed handler bridges.
	// Resources supplies immutable named filesystems for configured static mounts.
	Resources   *resource.Store
	Registry    *x.Registry
	Diagnostics io.Writer
}

type Server struct {
	mcpResourceAuthorizer auth.ResourceAuthorizer
	ready                 chan struct{}
	addresses             []string
	manager               *application.Manager
	source                *source
	servers               []*http.Server
	listeners             []net.Listener
	running               atomic.Bool
	shutdown              sync.Once
	done                  chan struct{}
	shutdownErr           error
	requests              sync.WaitGroup
	serving               sync.WaitGroup
	cancel                context.CancelFunc
	mu                    sync.Mutex
	closed                bool
}

func New(ctx context.Context, options Options) (_ *Server, err error) {
	if ctx == nil {
		return nil, fmt.Errorf("standalone context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolvedConfig, resolveErr := options.Config.ResolveConstants()
	if resolveErr != nil {
		return nil, resolveErr
	}
	options.Config = resolvedConfig
	if err := options.Config.Validate(); err != nil {
		return nil, err
	}
	if options.Config.Jobs != nil && (options.Async == nil || options.Async.Authorize == nil) {
		return nil, fmt.Errorf("standalone Jobs requires linked Async.Authorize; no default authorization is installed")
	}
	if options.Config.Jobs == nil && options.Async != nil {
		return nil, fmt.Errorf("linked Async options require Jobs configuration")
	}
	s := &Server{source: &source{Workspace: options.Workspace, config: options.Config, resources: options.Resources, holders: append([]any(nil), options.Holders...), requireLinked: options.RequireLinked || options.Holders != nil}, done: make(chan struct{}), ready: make(chan struct{}), mcpResourceAuthorizer: options.MCPResourceAuthorizer}
	s.source.codecFactories, err = normalizeCodecs(options.Codecs)
	if err != nil {
		return nil, err
	}
	types, err := s.source.init(ctx, options.Registry)
	if err != nil {
		return nil, err
	}
	if options.Config.Const != nil {
		if err = s.source.validateConstants(ctx, types); err != nil {
			return nil, err
		}
	}
	connections, err := connector.Open(ctx, options.Config.Connectors, options.Config.Connector)
	if err != nil {
		return nil, err
	}
	s.source.connections = connections
	defer func() {
		if err != nil {
			if s.manager != nil {
				_ = s.manager.Shutdown(context.Background())
			}
			_ = s.source.caches.Close()
			_ = connections.Close()
		}
	}()
	logger := serviceLogger(options.Diagnostics)
	if options.Diagnostics != nil {
		s.source.logger = logger
	}
	async, err := s.source.async(ctx, options.Async, logger)
	if err != nil {
		return nil, err
	}
	observation, err := s.source.services(ctx, logger)
	if err != nil {
		return nil, err
	}
	managerOptions := []application.Option{application.WithObservability(observation)}
	if async != nil {
		managerOptions = append(managerOptions, application.WithAsync(*async))
	}
	s.manager, err = application.New(types, managerOptions...)
	if err != nil {
		if observation.OTel != nil {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = observation.OTel.Exporter.Shutdown(cleanup)
		}
		return nil, err
	}
	return s, nil
}

// Reload reads the full authored source set again. Only Manager publishes it.
// Endpoint, credentials, connectors and other startup options are lifetime-fixed.
func (s *Server) Reload(ctx context.Context, revision uint64) error {
	if s == nil {
		return fmt.Errorf("standalone server is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return application.ErrClosed
	}
	return s.manager.Reload(ctx, application.Request{Revision: revision, Compile: s.source.compile})
}

// Serve binds every endpoint, publishes a complete initial generation and then
// serves. Readiness output contains only listener addresses, never configuration.
// The caller supplies signal cancellation; this package installs no global handlers.
func (s *Server) Serve(ctx context.Context, output io.Writer) (err error) {
	if s == nil || ctx == nil {
		return fmt.Errorf("standalone server and context are required")
	}
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("standalone server may only serve once")
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.source.config.Endpoint.ShutdownTimeout())
		defer cancel()
		err = errors.Join(err, s.Shutdown(shutdownCtx))
	}()
	if err = s.prepare(ctx); err != nil {
		return err
	}
	completed := make(chan error, len(s.servers))
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return application.ErrClosed
	}
	for index, server := range s.servers {
		listener := s.listeners[index]
		s.serving.Add(1)
		go func() { defer s.serving.Done(); completed <- server.Serve(listener) }()
		s.addresses = append(s.addresses, listener.Addr().String())
	}
	close(s.ready)
	s.mu.Unlock()
	if output != nil {
		for index, listener := range s.listeners {
			protocol := "HTTP"
			if index > 0 {
				protocol = "MCP"
			}
			if _, err = fmt.Fprintf(output, "%s listening on %s\n", protocol, listener.Addr()); err != nil {
				return err
			}
		}
	}
	select {
	case <-ctx.Done():
		return nil
	case err = <-completed:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// WaitReady returns owned listener addresses only after successful publication.
// The first address is HTTP; callers never need to parse diagnostic output.
func (s *Server) WaitReady(ctx context.Context) ([]string, error) {
	if s == nil || ctx == nil {
		return nil, fmt.Errorf("server and readiness context are required")
	}
	select {
	case <-s.ready:
		return append([]string(nil), s.addresses...), nil
	case <-s.done:
		return nil, fmt.Errorf("server stopped before readiness")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Metadata snapshots the published application generation, not source files.
func (s *Server) Metadata(ctx context.Context) (*application.Metadata, error) {
	return s.manager.Metadata(ctx)
}

func (s *Server) prepare(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return application.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	requestCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	endpoint := s.source.config.Endpoint
	address, _ := endpoint.ListenAddress()
	s.servers = []*http.Server{{Addr: address, Handler: s.manager, ReadHeaderTimeout: time.Duration(endpoint.ReadHeaderTimeoutMs) * time.Millisecond, IdleTimeout: time.Duration(endpoint.IdleTimeoutMs) * time.Millisecond, ReadTimeout: time.Duration(endpoint.ReadTimeoutMs) * time.Millisecond, WriteTimeout: time.Duration(endpoint.WriteTimeoutMs) * time.Millisecond, MaxHeaderBytes: endpoint.MaxHeaderBytes}}
	if policy := s.source.config.MCP; policy != nil {
		address, _ := policy.ListenAddress()
		protocol, createErr := mcpserver.New(mcpserver.Config{Source: s.manager, ResourceAuthorizer: s.mcpResourceAuthorizer, Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStreamable, Address: address, ReadHeaderTimeout: time.Duration(endpoint.ReadHeaderTimeoutMs) * time.Millisecond, IdleTimeout: time.Duration(endpoint.IdleTimeoutMs) * time.Millisecond}})
		if createErr != nil {
			return createErr
		}
		server, createErr := protocol.HTTP()
		if createErr != nil {
			return createErr
		}
		s.servers = append(s.servers, server)
	}
	for _, server := range s.servers {
		httpserver.Defaults(server)
		server.BaseContext = func(net.Listener) context.Context { return requestCtx }
		server.Handler = s.track(server.Handler)
		listener, listenErr := (&net.ListenConfig{}).Listen(ctx, "tcp", server.Addr)
		if listenErr != nil {
			return fmt.Errorf("standalone listener: %w", listenErr)
		}
		s.listeners = append(s.listeners, listener)
	}
	if err := s.manager.Reload(ctx, application.Request{Revision: 1, Compile: s.source.compile}); err != nil {
		return err
	}
	return s.exportDocuments(ctx)
}

func (s *Server) track(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			http.Error(w, "application unavailable", http.StatusServiceUnavailable)
			return
		}
		s.requests.Add(1)
		s.mu.Unlock()
		defer s.requests.Done()
		handler.ServeHTTP(w, r)
	})
}

// Shutdown drains transport requests before releasing Manager services and DBs.
// A caller deadline bounds waiting; cleanup continues and later calls join it.
// Call after Serve has returned or cancel Serve's context to initiate shutdown.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("shutdown context is required")
	}
	s.shutdown.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		go func() {
			deadline, cancel := context.WithTimeout(context.Background(), s.source.config.Endpoint.ShutdownTimeout())
			defer cancel()
			for _, server := range s.servers {
				if err := server.Shutdown(deadline); err != nil {
					s.shutdownErr = errors.Join(s.shutdownErr, err)
					_ = server.Close()
				}
			}
			for _, listener := range s.listeners {
				_ = listener.Close()
			}
			if s.cancel != nil {
				s.cancel()
			}
			s.serving.Wait()
			s.requests.Wait()
			s.shutdownErr = errors.Join(s.shutdownErr, s.manager.Shutdown(context.Background()), s.source.caches.Close(), s.source.connections.Close())
			close(s.done)
		}()
	})
	select {
	case <-s.done:
		return s.shutdownErr
	default:
	}
	select {
	case <-s.done:
		return s.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ServeHTTP serves the published generation. Reload must succeed before serving.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.track(s.manager).ServeHTTP(w, r)
}
