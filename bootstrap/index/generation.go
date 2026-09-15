package index

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

var (
	ErrClosed      = errors.New("bootstrap component generation is closed")
	ErrStaleSource = errors.New("bootstrap component source changed after indexing")
)

// Loaded is one generation-owned executable component and its cleanup hook.
// Release must close only resources owned by this materialization.
type Loaded struct {
	Registration *registry.RegisteredComponent
	// Related registrations are materialized coherently with the primary (for
	// example report-generated subcomponents) and become exact-key load hits in
	// this generation.
	Related []*registry.RegisteredComponent
	Release func(context.Context) error
}

// Resolver lets a materializer load component dependencies from the same
// immutable generation. Cycles fail before waiting on an existing load.
type Resolver interface {
	Load(context.Context, spec.Key) (*Loaded, error)
}

type Materializer interface {
	Materialize(context.Context, *Entry, Resolver) (*Loaded, error)
}

type MaterializeFunc func(context.Context, *Entry, Resolver) (*Loaded, error)

func (f MaterializeFunc) Materialize(ctx context.Context, entry *Entry, resolver Resolver) (*Loaded, error) {
	return f(ctx, entry, resolver)
}

type loadState struct {
	done   chan struct{}
	loaded *Loaded
	err    error
}

type loadStackKey struct{}

type Generation struct {
	ID           uint64
	Snapshot     *Snapshot
	materializer Materializer
	ctx          context.Context
	cancel       context.CancelFunc
	refs         atomic.Int64
	retired      atomic.Bool
	mu           sync.Mutex
	loads        map[string]*loadState
	loadWG       sync.WaitGroup
	closed       chan struct{}
	closeOnce    sync.Once
	closeErr     error
}

func newGeneration(id uint64, snapshot *Snapshot, materializer Materializer) *Generation {
	ctx, cancel := context.WithCancel(context.Background())
	result := &Generation{ID: id, Snapshot: snapshot, materializer: materializer, ctx: ctx, cancel: cancel, loads: map[string]*loadState{}, closed: make(chan struct{})}
	result.refs.Store(1) // registry ownership
	return result
}

func (g *Generation) acquire() bool {
	if g == nil || g.retired.Load() {
		return false
	}
	g.refs.Add(1)
	if g.retired.Load() {
		g.release()
		return false
	}
	return true
}

func (g *Generation) retire() {
	if g != nil && g.retired.CompareAndSwap(false, true) {
		g.release()
	}
}

func (g *Generation) release() {
	if g.refs.Add(-1) == 0 {
		go g.close()
	}
}

func (g *Generation) close() {
	g.closeOnce.Do(func() {
		g.cancel()
		g.loadWG.Wait()
		g.mu.Lock()
		keys := make([]string, 0, len(g.loads))
		for key, state := range g.loads {
			if state != nil && state.loaded != nil && state.loaded.Release != nil {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		callbacks := make([]func(context.Context) error, 0, len(keys))
		for _, key := range keys {
			callbacks = append(callbacks, g.loads[key].loaded.Release)
		}
		g.mu.Unlock()
		for index, release := range callbacks {
			if err := release(context.Background()); err != nil {
				g.closeErr = errors.Join(g.closeErr, fmt.Errorf("release component %s: %w", keys[index], err))
			}
		}
		close(g.closed)
	})
}

func (g *Generation) waitClosed(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case <-g.closed:
		return g.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *Generation) Load(ctx context.Context, key spec.Key) (*Loaded, error) {
	if ctx == nil {
		return nil, fmt.Errorf("component load context is required")
	}
	if g == nil || g.Snapshot == nil || g.materializer == nil {
		return nil, fmt.Errorf("component generation materializer is required")
	}
	identity := key.String()
	stack, _ := ctx.Value(loadStackKey{}).([]string)
	for _, active := range stack {
		if active == identity {
			return nil, fmt.Errorf("component materialization cycle: %s", strings.Join(append(append([]string(nil), stack...), identity), " -> "))
		}
	}
	g.mu.Lock()
	state := g.loads[identity]
	if state == nil {
		entry, ok := g.Snapshot.component(key)
		if !ok {
			g.mu.Unlock()
			return nil, fmt.Errorf("indexed component not found: %s", key.String())
		}
		state = &loadState{done: make(chan struct{})}
		g.loads[identity] = state
		g.loadWG.Add(1)
		loadCtx := context.WithValue(g.ctx, loadStackKey{}, append(append([]string(nil), stack...), identity))
		go g.materialize(loadCtx, entry, state)
	}
	g.mu.Unlock()
	select {
	case <-state.done:
		return state.loaded, state.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (g *Generation) materialize(ctx context.Context, entry *Entry, state *loadState) {
	defer func() {
		if recovered := recover(); recovered != nil {
			state.loaded = nil
			state.err = fmt.Errorf("materialize component %s: panic: %v", entry.Key().String(), recovered)
		}
		close(state.done)
		g.loadWG.Done()
	}()
	if err := verifySources(entry); err != nil {
		state.err = err
		return
	}
	loaded, err := g.materializer.Materialize(ctx, entry.Clone(), g)
	if err == nil {
		err = validateLoaded(entry, loaded)
	}
	if err != nil && loaded != nil && loaded.Release != nil {
		_ = loaded.Release(context.Background())
		loaded = nil
	}
	state.loaded, state.err = loaded, err
	if err == nil {
		g.mu.Lock()
		for _, related := range loaded.Related {
			if related == nil || related.Component == nil {
				continue
			}
			identity := related.Component.Key.String()
			if g.loads[identity] != nil {
				continue
			}
			done := make(chan struct{})
			close(done)
			g.loads[identity] = &loadState{done: done, loaded: &Loaded{Registration: related}}
		}
		g.mu.Unlock()
	}
}

func verifySources(entry *Entry) error {
	for _, source := range entry.Sources {
		content, err := os.ReadFile(source.Path)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrStaleSource, source.Path)
		}
		digest := sha256.Sum256(content)
		if hex.EncodeToString(digest[:]) != source.Fingerprint {
			return fmt.Errorf("%w: %s", ErrStaleSource, source.Path)
		}
	}
	return nil
}

func validateLoaded(entry *Entry, loaded *Loaded) error {
	if loaded == nil || loaded.Registration == nil || loaded.Registration.Component == nil {
		return fmt.Errorf("materialized component registration is required")
	}
	actual := loaded.Registration.Component
	if actual.Key != entry.Component.Key {
		return fmt.Errorf("materialized component key %s does not match indexed key %s", actual.Key.String(), entry.Component.Key.String())
	}
	indexed := map[string]bool{}
	for _, endpoint := range entry.Component.Routes {
		if endpoint != nil {
			indexed[(spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}).String()] = true
		}
	}
	for _, endpoint := range actual.Routes {
		if endpoint != nil {
			delete(indexed, (spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}).String())
		}
	}
	if len(indexed) != 0 {
		return fmt.Errorf("materialized component %s does not own every indexed route", actual.Key.String())
	}
	return nil
}
