// Package client owns the default outbound client implementations. Registry
// resolves one client per distinct typed options value, reuses it across
// invocations and components, and releases everything on Close. It is the
// single lifecycle boundary; borrowers never close clients.
package client

import (
	"context"
	"fmt"
	"sync"

	ihttp "github.com/viant/datly/internal/client/http"
	imcp "github.com/viant/datly/internal/client/mcp"
	"github.com/viant/datly/internal/client/support"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

type closer interface{ Close() error }

// Registry caches HTTP and MCP clients keyed by option identity.
type Registry struct {
	mu     sync.Mutex
	closed bool
	http   map[string]*ihttp.Client
	mcp    map[string]*imcp.Client
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{http: map[string]*ihttp.Client{}, mcp: map[string]*imcp.Client{}}
}

// HTTP exposes the registry as the public HTTP provider contract.
func (r *Registry) HTTP() xhttp.Provider { return httpProvider{registry: r} }

// MCP exposes the registry as the public MCP provider contract.
func (r *Registry) MCP() xmcp.Provider { return mcpProvider{registry: r} }

type httpProvider struct{ registry *Registry }

func (p httpProvider) Client(ctx context.Context, options xhttp.Options) (xhttp.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ihttp.Validate(options); err != nil {
		return nil, err
	}
	identity, err := support.Identity(options)
	if err != nil {
		return nil, err
	}
	r := p.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("client registry is closed")
	}
	if existing, ok := r.http[identity]; ok {
		return existing, nil
	}
	created, err := ihttp.New(options)
	if err != nil {
		return nil, err
	}
	r.http[identity] = created
	return created, nil
}

type mcpProvider struct{ registry *Registry }

func (p mcpProvider) Client(ctx context.Context, options xmcp.Options) (xmcp.Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := imcp.Validate(options); err != nil {
		return nil, err
	}
	identity, err := support.Identity(options)
	if err != nil {
		return nil, err
	}
	r := p.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("client registry is closed")
	}
	if existing, ok := r.mcp[identity]; ok {
		return existing, nil
	}
	created, err := imcp.New(options)
	if err != nil {
		return nil, err
	}
	r.mcp[identity] = created
	return created, nil
}

// Size reports the number of distinct configured clients held.
func (r *Registry) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.http) + len(r.mcp)
}

// Close releases every client (MCP sessions included) and refuses further
// resolution.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	var failure error
	closeAll := func(items map[string]closer) {
		for identity, item := range items {
			if err := item.Close(); err != nil && failure == nil {
				failure = err
			}
			delete(items, identity)
		}
	}
	httpItems := make(map[string]closer, len(r.http))
	for identity, item := range r.http {
		httpItems[identity] = item
	}
	mcpItems := make(map[string]closer, len(r.mcp))
	for identity, item := range r.mcp {
		mcpItems[identity] = item
	}
	closeAll(httpItems)
	closeAll(mcpItems)
	r.http, r.mcp = map[string]*ihttp.Client{}, map[string]*imcp.Client{}
	return failure
}
