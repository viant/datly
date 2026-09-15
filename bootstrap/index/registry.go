package index

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/viant/datly/spec"
)

// Registry atomically publishes complete bootstrap index generations. A Lease
// pins one generation for the full request, including lazy dependency loads.
type Registry struct {
	active atomic.Pointer[Generation]
	next   atomic.Uint64
	closed atomic.Bool
	mu     sync.Mutex
}

func (r *Registry) Publish(snapshot *Snapshot, materializer Materializer) (*Generation, error) {
	if r == nil || snapshot == nil || materializer == nil {
		return nil, fmt.Errorf("bootstrap snapshot and materializer are required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed.Load() {
		return nil, ErrClosed
	}
	next := newGeneration(r.next.Add(1), snapshot, materializer)
	previous := r.active.Swap(next)
	if previous != nil {
		previous.retire()
	}
	return next, nil
}

type Lease struct {
	generation *Generation
	once       sync.Once
}

func (r *Registry) Pin() (*Lease, error) {
	if r == nil || r.closed.Load() {
		return nil, ErrClosed
	}
	for {
		current := r.active.Load()
		if current == nil {
			return nil, fmt.Errorf("bootstrap index has no published generation")
		}
		if current.acquire() {
			return &Lease{generation: current}, nil
		}
	}
}

func (l *Lease) Close() {
	if l != nil {
		l.once.Do(func() { l.generation.release() })
	}
}

func (l *Lease) GenerationID() uint64 {
	if l == nil || l.generation == nil {
		return 0
	}
	return l.generation.ID
}

func (l *Lease) Snapshot() *Snapshot {
	if l == nil {
		return nil
	}
	return l.generation.Snapshot
}

func (l *Lease) LoadComponent(ctx context.Context, key spec.Key) (*Loaded, error) {
	if l == nil || l.generation == nil {
		return nil, ErrClosed
	}
	return l.generation.Load(ctx, key)
}

// Gate runs against indexed route metadata before component materialization.
// Protocol adapters use it for route/API authorization; canonical binding and
// handler authorization continue against the loaded registration.
type Gate func(context.Context, *Entry, *spec.Route) error

func (l *Lease) LoadRoute(ctx context.Context, method, path string, gate Gate) (*Loaded, map[string]string, error) {
	if l == nil || l.generation == nil {
		return nil, nil, ErrClosed
	}
	entry, endpoint, params, ok := l.generation.Snapshot.route(method, path)
	if !ok {
		return nil, nil, fmt.Errorf("indexed route not found: %s %s", method, path)
	}
	if gate != nil {
		if err := gate(ctx, entry.Clone(), endpoint.Clone()); err != nil {
			return nil, nil, err
		}
	}
	loaded, err := l.generation.Load(ctx, entry.Component.Key)
	return loaded, params, err
}

func (l *Lease) LoadMCP(ctx context.Context, identity MCPIdentity, gate Gate) (*Loaded, error) {
	if l == nil || l.generation == nil {
		return nil, ErrClosed
	}
	entry, ok := l.generation.Snapshot.byMCP[identity.key()]
	if !ok {
		return nil, fmt.Errorf("indexed MCP %s not found: %s", identity.Kind, identity.Name)
	}
	var endpoint *spec.Route
	for _, route := range entry.Component.Routes {
		for _, exposure := range route.MCP {
			if exposure != nil && exposure.Kind == identity.Kind && exposure.Identity(entry.Component, route) == identity.Name {
				endpoint = route
				break
			}
		}
	}
	if gate != nil {
		if err := gate(ctx, entry.Clone(), endpoint.Clone()); err != nil {
			return nil, err
		}
	}
	return l.generation.Load(ctx, entry.Component.Key)
}

// MaterializeAll is the explicit opt-in for schema/documentation consumers
// that require full contracts. Normal bootstrap and metadata listing use the
// index and never call this method.
func (l *Lease) MaterializeAll(ctx context.Context) ([]*Loaded, error) {
	if l == nil || l.generation == nil {
		return nil, ErrClosed
	}
	entries := l.generation.Snapshot.entries
	result := make([]*Loaded, 0, len(entries))
	for _, entry := range entries {
		loaded, err := l.generation.Load(ctx, entry.Component.Key)
		if err != nil {
			return nil, err
		}
		result = append(result, loaded)
	}
	return result, nil
}

func (r *Registry) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("bootstrap shutdown context is required")
	}
	r.mu.Lock()
	r.closed.Store(true)
	current := r.active.Swap(nil)
	if current != nil {
		current.retire()
	}
	r.mu.Unlock()
	return current.waitClosed(ctx)
}
