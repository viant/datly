package http

import (
	"context"
	"fmt"
	stdhttp "net/http"
	"os"
	"strings"
	"time"

	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xlogger "github.com/viant/xdatly/logger"
)

// Config is the application HTTP policy. A nil CORS policy selects
// safe noncredentialed defaults; an explicit empty CORS policy remains empty.
type Config struct {
	// Metrics opts into diagnostic response headers. Nil disables them.
	Metrics *MetricsConfig `json:"Metrics,omitempty" yaml:"Metrics,omitempty"`
	Async   []AsyncRoute   `json:"Async,omitempty" yaml:"Async,omitempty"`
	// StaticLocalRoot is an optional caller-owned local filesystem authority.
	// Configured ContentURL paths beneath it must be relative and symlink-free.
	// Keep it open until all reloads finish. JSON configuration cannot grant it.
	StaticLocalRoot *os.Root `json:"-"`
	StaticContent   []*spec.StaticContent
	ContentURL      string
	APIKeys         APIKeys        `json:"APIKeys,omitempty" yaml:"APIKeys,omitempty"`
	DisableCors     bool           `json:"DisableCors,omitempty" yaml:"DisableCors,omitempty"`
	CORS            *spec.CORS     `json:"CORS,omitempty" yaml:"CORS,omitempty"`
	APIPrefix       string         `json:"APIPrefix,omitempty" yaml:"APIPrefix,omitempty"`
	Meta            Meta           `json:"Meta,omitempty" yaml:"Meta,omitempty"`
	OpenAPI         *OpenAPIConfig `json:"OpenAPI,omitempty" yaml:"OpenAPI,omitempty"`
	Warmup          *WarmupConfig  `json:"-" yaml:"-"`
	// Authorize applies application policy to every resolved component target
	// after route/API-key checks and before request binding or execution.
	Authorize func(context.Context, *stdhttp.Request, dexec.ComponentTarget) error `json:"-" yaml:"-"`
}

const DefaultCacheWarmURI = "/v1/api/cache/warmup"

// Meta preserves original Init semantics: empty selects the default URI;
// a nonempty whitespace value explicitly disables warmup route activation.
type Meta struct {
	// AllowedSubnet retains original RemoteAddr prefix matching (not CIDR parsing).
	AllowedSubnet []string `json:"AllowedSubnet,omitempty" yaml:"AllowedSubnet,omitempty"`
	OpenApiURI    string   `json:"OpenApiURI,omitempty" yaml:"OpenApiURI,omitempty"`
	DocURI        string   `json:"DocURI,omitempty" yaml:"DocURI,omitempty"`
	CacheWarmURI  string   `json:"CacheWarmURI,omitempty" yaml:"CacheWarmURI,omitempty"`
}

func (c Config) resolved() Config {
	c.Meta.AllowedSubnet = append([]string(nil), c.Meta.AllowedSubnet...)
	if c.DisableCors {
		c.CORS = nil
	} else if c.CORS == nil {
		c.CORS = spec.DefaultCORS()
	} else {
		c.CORS = c.CORS.Resolve(nil)
	}
	if c.Meta.OpenApiURI == "" {
		c.Meta.OpenApiURI = DefaultOpenAPIURI
	}
	if c.OpenAPI != nil && c.Meta.DocURI == "" {
		c.Meta.DocURI = DefaultDocURI
	}
	if c.Meta.CacheWarmURI == "" {
		c.Meta.CacheWarmURI = DefaultCacheWarmURI
	}
	if c.Warmup != nil {
		policy := *c.Warmup
		policy.AdminHeaders = append([]string(nil), c.Warmup.AdminHeaders...)
		c.Warmup = &policy
	}
	return c
}

func (c Config) routeCORS(endpoint *spec.Route) *spec.CORS {
	if c.DisableCors {
		return nil
	}
	return endpoint.CORS.Resolve(c.CORS)
}

// WarmupConfig supplies server lifetime and explicit administration policy.
// Authorize must authorize this exact target; a successful component JWT alone
// does not grant cache administration. Lifetime must be server-owned.
type WarmupConfig struct {
	Lifetime  *WarmupLifetime
	Timeout   time.Duration
	Authorize func(context.Context, *stdhttp.Request, dexec.ComponentTarget) error
	// AdminHeaders are the extra request headers available to Authorize. API-key
	// and canonical JWT headers are copied automatically; bodies are never retained.
	AdminHeaders []string
	// Completed receives preparation failures and every dispatched operation outcome,
	// even after client cancellation. Callbacks must support concurrent requests.
	Completed func(WarmupResult, error)
}

// HandlerInput carries canonical registrations only during HTTP staging.
// They are consumed to freeze documents, never exposed as a mutable registry.
type HandlerInput struct {
	Async      AsyncAdmission
	Runtime    *druntime.Runtime
	Components []*registry.RegisteredComponent
	Logger     xlogger.Logger
	Version    string
}

func (c Config) NewHandler(rt *druntime.Runtime, log xlogger.Logger, version string) (*Handler, error) {
	return c.Build(context.Background(), HandlerInput{Runtime: rt, Logger: log, Version: version})
}

// Build validates policy and freezes configured metadata from the registrations
// used by Runtime. Call it while staging, before publishing the returned handler.
func (c Config) Build(ctx context.Context, input HandlerInput) (*Handler, error) {
	if ctx == nil {
		return nil, fmt.Errorf("HTTP staging context is required")
	}
	rt, log, version := input.Runtime, input.Logger, input.Version

	if rt == nil {
		return nil, fmt.Errorf("HTTP runtime is required")
	}
	c = c.resolved()
	keys, err := c.APIKeys.Resolve(ctx)
	if err != nil {
		return nil, err
	}
	for _, endpoint := range rt.Routes() {
		if key := keys.match(endpoint.Path); key != nil && (endpoint.APIKeyHeader != key.Header || endpoint.APIKeyValue != key.Value) {
			return nil, fmt.Errorf("APIKeys must be staged on canonical routes through application.Manager")
		}
	}
	h := NewHandler(rt, log, version)
	h.authorize = c.Authorize
	h.allowedSubnet = c.Meta.AllowedSubnet
	if c.Metrics != nil {
		policy := *c.Metrics
		h.metrics = &policy
	}
	h.cors = map[string]*corsPolicy{}
	for _, endpoint := range rt.Routes() {
		policy, err := newCORSPolicy(c.routeCORS(endpoint))
		if err != nil {
			return nil, fmt.Errorf("CORS %s %s: %w", endpoint.Method, endpoint.Path, err)
		}
		h.cors[(spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}).String()] = policy
	}
	if strings.TrimSpace(c.Meta.CacheWarmURI) != "" {
		warmup, err := newWarmupRoutes(rt, c)
		if err != nil {
			return nil, err
		}
		if warmup != nil && warmup.policy.Completed == nil && log == nil {
			return nil, fmt.Errorf("HTTP warmup requires a completion callback or logger")
		}
		h.warmup = warmup
		h.warmupPrefix = strings.TrimSuffix(c.Meta.CacheWarmURI, "/")
	}
	h.async, err = h.newAsyncRoutes(c.Async, input)
	if err != nil {
		return nil, err
	}
	h.documents, err = c.documents(ctx, input)
	if err != nil {
		return nil, err
	}
	h.static, err = c.staticRoutes(ctx, input, keys)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// Shutdown stops accepting warmups, cancels server operations, and waits for
// their completion up to ctx's deadline. Call alongside http.Server.Shutdown.
func (h *Handler) Shutdown(ctx context.Context) error {
	if h == nil || h.warmup == nil {
		return nil
	}
	return h.warmup.lifetime.Shutdown(ctx)
}
