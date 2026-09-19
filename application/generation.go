// Package application stages and atomically publishes complete executable
// generations. Runtime, HTTP, MCP and type authority share one publication.
package application

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/viant/bindly/resource"
	documentation "github.com/viant/datly/documentation"
	xdocs "github.com/viant/xdatly/docs"

	bootstrapindex "github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp/server/auth"
	xlogger "github.com/viant/xdatly/logger"
)

var ErrStale = errors.New("application generation is stale")

// Build is the complete result of trusted stage composition. Components must be
// newly compiled; shared dependencies and providers must be immutable. Successful
// publication transfers ownership: callers must not mutate the returned carriers.
// MCP.Components and Invoker are owned by publication and must be left empty.
type Build struct {
	Documentation xdocs.Source
	Components    []*registry.RegisteredComponent
	// Observability is application-owned; configure it through application.New.
	// Stage-local observation options are rejected before constructing a runtime.
	RuntimeOptions []runtime.Option
	MCP            mcp.Config
	// HTTP is validated with the rest of the stage before publication. Manager
	// supplies its shared server lifetime; stages must not supply Warmup.Lifetime.
	HTTP    gateway.Config
	Logger  xlogger.Logger
	Version string
	// Resources is the stage-owned filesystem authority shared by all compilers,
	// runtime binding and MCP. Conflicting explicitly configured stores fail.
	Resources *resource.Store
	// Types is the actual staged catalog consumed by component compilation when
	// that compiler creates its own detached catalog. Nil uses Compile's input.
	Types *typecatalog.Catalog
	// Index publishes bootstrap route/MCP metadata without compiling every
	// component. Materializer is invoked once per used component and generation.
	Index        *bootstrapindex.Snapshot
	Materializer bootstrapindex.Materializer
	// Preload names only explicitly configured startup consumers such as cache
	// warmup or async routes; unrelated indexed components remain lazy.
	Preload []spec.Key
	// Shutdown owns stage-local external resources such as database handles.
	// After publication, Manager calls it exactly once when the generation has
	// retired and all admitted requests have released their generation lease. A
	// failed stage is also shut down before Reload returns.
	Shutdown func(context.Context) error
}

type Request struct {
	// Revision is the monotonic revision of the complete authored source set.
	Revision uint64
	// Compile receives a detached catalog. It must compile the complete source
	// set and all plans before returning, without side effects on active services.
	Compile func(context.Context, *typecatalog.Catalog) (*Build, error)
}

type generation struct {
	revision   uint64
	types      *typecatalog.Catalog
	runtime    *runtime.Runtime
	http       *gateway.Handler
	httpConfig gateway.Config
	mcp        *mcp.Service
	sources    map[*spec.Component]bool
	async      *asyncGeneration
	index      *bootstrapindex.Registry
	lease      *bootstrapindex.Lease
	useMu      sync.Mutex
	uses       int
	retiring   bool
	closed     chan struct{}
	closeErr   error
	closeOnce  sync.Once
	shutdown   func(context.Context) error
	httpMu     sync.Mutex
	documents  func(context.Context) (*gateway.Handler, error)
	docsReady  bool
}

type Manager struct {
	active          atomic.Pointer[generation]
	seed            *typecatalog.Catalog
	publication     sync.Mutex
	stopped         atomic.Bool
	warmups         *gateway.WarmupLifetime
	observation     *runtime.Observability
	async           *asyncLifetime
	shutdownDone    chan struct{}
	shutdownErr     error
	externalPins    map[uint64]func()
	nextExternalPin uint64
}

// New snapshots the base package type authority used by every staged source set.
// Types authored by reloadable DQL belong in Request.Compile, not this seed.
func New(types *typecatalog.Catalog, configure ...Option) (*Manager, error) {
	options := &options{}
	for _, option := range configure {
		if option != nil {
			if err := option(options); err != nil {
				return nil, err
			}
		}
	}
	if types == nil {
		types = typecatalog.NewCatalog()
	}
	seed, err := types.Clone()
	if err != nil {
		return nil, err
	}
	manager := &Manager{seed: seed}
	manager.async, err = manager.newAsync(options.async)
	if err != nil {
		return nil, err
	}
	observation, err := runtime.NewObservability(options.observability)
	if err != nil {
		manager.async.stop()
		return nil, err
	}
	manager.observation = observation
	manager.warmups = gateway.NewWarmupLifetime(context.Background())
	return manager, nil
}

func (m *Manager) Revision() uint64 {
	if m == nil {
		return 0
	}
	if current := m.active.Load(); current != nil {
		return current.revision
	}
	return 0
}

// Reload stages off to the side and publishes once. Concurrent superseded
// stages fail rather than overwriting a newer source generation.
func (m *Manager) Reload(ctx context.Context, request Request) error {
	if m == nil || m.seed == nil || request.Compile == nil || ctx == nil {
		return fmt.Errorf("application stage compiler is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.stopped.Load() {
		return ErrClosed
	}
	previous := m.active.Load()
	if request.Revision == 0 || previous != nil && request.Revision <= previous.revision {
		return ErrStale
	}
	// Every source set starts from package authority, not generated descriptors
	// from the previous DQL set; removed/renamed authored types cannot survive.
	types, err := m.seed.Clone()
	if err != nil {
		return err
	}
	built, err := request.Compile(ctx, types)
	if err != nil {
		return err
	}
	if m.stopped.Load() {
		return ErrClosed
	}
	if built == nil {
		return fmt.Errorf("application stage is required")
	}
	buildOwned := true
	defer func() {
		if buildOwned && built.Shutdown != nil {
			_ = built.Shutdown(context.Background())
		}
	}()
	if built.MCP.Invoker != nil || len(built.MCP.Components) != 0 {
		return fmt.Errorf("application owns MCP registration and invocation")
	}
	identities := map[string]bool{}
	keys, err := built.HTTP.APIKeys.Resolve(ctx)
	if err != nil {
		return err
	}
	staged := *built
	staged.HTTP.APIKeys = keys
	built = &staged
	var indexed []*spec.Component
	var indexRegistry *bootstrapindex.Registry
	var indexLease *bootstrapindex.Lease
	cleanupIndex := true
	defer func() {
		if cleanupIndex && indexRegistry != nil {
			if indexLease != nil {
				indexLease.Close()
			}
			_ = indexRegistry.Shutdown(context.Background())
		}
	}()
	if built.Index != nil {
		if built.Materializer == nil || len(built.Components) != 0 {
			return fmt.Errorf("indexed application requires one materializer and no eager components")
		}
		transformed, transformErr := built.Index.Transform(keys.Apply)
		if transformErr != nil {
			return transformErr
		}
		built.Index = transformed
		for _, entry := range transformed.Entries() {
			indexed = append(indexed, entry.Component)
		}
		indexRegistry = &bootstrapindex.Registry{}
		if _, err = indexRegistry.Publish(built.Index, stagedIndexMaterializer{base: built.Materializer, keys: keys}); err != nil {
			return err
		}
		indexLease, err = indexRegistry.Pin()
		if err != nil {
			return err
		}
	}
	if built.Index == nil && len(built.Preload) != 0 {
		return fmt.Errorf("application component preload requires an index")
	}
	sources := map[*spec.Component]bool{}
	rawComponents := append([]*registry.RegisteredComponent(nil), built.Components...)
	if built.Index != nil {
		loaded, loadErr := preloadComponents(ctx, indexLease, built.Preload)
		if loadErr != nil {
			return loadErr
		}
		rawComponents = append(rawComponents, loaded...)
	}
	components := make([]*registry.RegisteredComponent, 0, len(rawComponents))
	for _, component := range rawComponents {
		if component == nil || component.Component == nil {
			return fmt.Errorf("application component is required")
		}
		key := component.Component.Key.String()
		if identities[key] {
			return fmt.Errorf("duplicate application component %s", key)
		}
		identities[key] = true
		if previous != nil && previous.sources[component.Component] {
			return fmt.Errorf("application stage reused active component %s", key)
		}
		sources[component.Component] = true
		entry := *component
		entry.Component = component.Component.Clone()
		keys.Apply(entry.Component)
		components = append(components, &entry)
	}
	resources := built.Resources
	if supplied := built.MCP.Resources; supplied != nil {
		if resources != nil && resources != supplied {
			return fmt.Errorf("application MCP resource store conflicts with stage resources")
		}
		resources = supplied
	}
	loader := documentation.Loader{Resources: resources}
	global, err := loader.Load(ctx, built.Documentation)
	if err != nil {
		return err
	}
	for _, entry := range components {
		snapshot := entry.Documentation
		if !built.Documentation.IsZero() || snapshot == nil {
			snapshot, err = loader.Overlay(ctx, global, entry.Component.Documentation)
		}
		if err != nil {
			return fmt.Errorf("component %s: %w", entry.Component.Key.String(), err)
		}
		snapshot, err = snapshot.ForComponent(entry.Component, entry.OutputType)
		if err != nil {
			return err
		}
		entry.Documentation = snapshot
		entry.Input = entry.Input.WithDocumentation(snapshot)
	}
	runtimeOptions := append([]runtime.Option(nil), built.RuntimeOptions...)
	runtimeOptions = append(runtimeOptions, runtime.WithManagedObservability(m.observation))
	if resources != nil {
		runtimeOptions = append(runtimeOptions, runtime.WithResources(resources))
	}
	var rt *runtime.Runtime
	if built.Index != nil {
		rt, err = runtime.NewIndexedRuntime(indexed, components, indexedRuntimeLoader{lease: indexLease}, runtimeOptions...)
	} else {
		rt, err = runtime.NewRuntime(components, runtimeOptions...)
	}
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = rt.Shutdown(context.Background())
		}
	}()
	config := built.MCP
	config.Components, config.Invoker = components, rt
	config.Indexed, config.Loader = indexed, rt
	config.Resources = rt.Resources()
	service, err := config.Compile(ctx)
	if err != nil {
		return err
	}
	if policy := service.Authorization(); policy != nil {
		if _, err := auth.New(&auth.Config{Policy: policy}); err != nil {
			return err
		}
	}
	// Do not expose the callback's mutable catalog through the generation.
	if built.Types != nil {
		types = built.Types
	}
	types, err = types.Clone()
	if err != nil {
		return err
	}
	async, err := m.async.generation(rt)
	if err != nil {
		return err
	}
	httpBuild := built
	if built.Index != nil && built.HTTP.OpenAPI != nil {
		copy := *built
		copy.HTTP = built.HTTP
		copy.HTTP.OpenAPI = nil
		httpBuild = &copy
	}
	httpHandler, err := m.newHTTPHandler(ctx, httpBuild, gateway.HandlerInput{Async: m.jobAdmission(async), Runtime: rt, Components: components, Logger: built.Logger, Version: built.Version})
	if err != nil {
		return err
	}
	next := &generation{async: async, revision: request.Revision, types: types, runtime: rt, http: httpHandler, httpConfig: built.HTTP, mcp: service, sources: sources, index: indexRegistry, lease: indexLease, closed: make(chan struct{}), shutdown: built.Shutdown}
	if built.Index != nil && built.HTTP.OpenAPI != nil {
		next.documents = func(loadCtx context.Context) (*gateway.Handler, error) {
			var registrations []*registry.RegisteredComponent
			seenRegistrations := map[string]bool{}
			for _, entry := range built.Index.Entries() {
				family, loadErr := rt.LoadComponents(loadCtx, entry.Key())
				if loadErr != nil {
					return nil, loadErr
				}
				for _, registration := range family {
					if registration == nil || registration.Component == nil || seenRegistrations[registration.Component.Key.String()] {
						continue
					}
					seenRegistrations[registration.Component.Key.String()] = true
					registrations = append(registrations, registration)
				}
			}
			documented := make([]*registry.RegisteredComponent, 0, len(registrations))
			for _, registration := range registrations {
				entry := *registration
				snapshot := entry.Documentation
				var loadErr error
				if !built.Documentation.IsZero() || snapshot == nil {
					snapshot, loadErr = loader.Overlay(loadCtx, global, entry.Component.Documentation)
				}
				if loadErr == nil {
					snapshot, loadErr = snapshot.ForComponent(entry.Component, entry.OutputType)
				}
				if loadErr != nil {
					return nil, loadErr
				}
				entry.Documentation = snapshot
				entry.Input = entry.Input.WithDocumentation(snapshot)
				documented = append(documented, &entry)
			}
			return m.newHTTPHandler(loadCtx, built, gateway.HandlerInput{Async: m.jobAdmission(async), Runtime: rt, Components: documented, Logger: built.Logger, Version: built.Version})
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.publication.Lock()
	defer m.publication.Unlock()
	if m.stopped.Load() {
		return ErrClosed
	}
	if !m.active.CompareAndSwap(previous, next) {
		return ErrStale
	}
	if previous != nil {
		previous.retire()
	}
	published = true
	buildOwned = false
	cleanupIndex = false
	m.async.start()
	return nil
}

type stagedIndexMaterializer struct {
	base bootstrapindex.Materializer
	keys gateway.APIKeys
}

func (m stagedIndexMaterializer) Materialize(ctx context.Context, entry *bootstrapindex.Entry, resolver bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
	loaded, err := m.base.Materialize(ctx, entry, resolver)
	if err != nil || loaded == nil || loaded.Registration == nil {
		return loaded, err
	}
	copy := *loaded.Registration
	copy.Component = loaded.Registration.Component.Clone()
	m.keys.Apply(copy.Component)
	result := *loaded
	result.Registration = &copy
	result.Related = make([]*registry.RegisteredComponent, len(loaded.Related))
	for index, related := range loaded.Related {
		if related == nil || related.Component == nil {
			continue
		}
		relatedCopy := *related
		relatedCopy.Component = related.Component.Clone()
		m.keys.Apply(relatedCopy.Component)
		result.Related[index] = &relatedCopy
	}
	return &result, nil
}

func (g *generation) ensureDocuments(ctx context.Context) error {
	if g == nil || g.documents == nil {
		return nil
	}
	g.httpMu.Lock()
	defer g.httpMu.Unlock()
	if g.docsReady {
		return nil
	}
	handler, err := g.documents(ctx)
	if err != nil {
		return err
	}
	g.http = handler
	g.docsReady = true
	return nil
}

func (g *generation) acquire() bool {
	if g == nil {
		return false
	}
	g.useMu.Lock()
	defer g.useMu.Unlock()
	if g.retiring && g.index != nil {
		return false
	}
	g.uses++
	return true
}

func (g *generation) release() {
	if g == nil {
		return
	}
	g.useMu.Lock()
	g.uses--
	closeNow := g.retiring && g.uses == 0
	g.useMu.Unlock()
	if closeNow {
		go g.closeResources()
	}
}

func (g *generation) retire() {
	if g == nil {
		return
	}
	g.useMu.Lock()
	g.retiring = true
	closeNow := g.uses == 0
	g.useMu.Unlock()
	if closeNow {
		go g.closeResources()
	}
}

func (g *generation) closeResources() {
	if g == nil {
		return
	}
	g.closeOnce.Do(func() {
		if g.runtime != nil {
			g.closeErr = errors.Join(g.closeErr, g.runtime.Shutdown(context.Background()))
		}
		if g.index != nil {
			g.lease.Close()
			g.closeErr = errors.Join(g.closeErr, g.index.Shutdown(context.Background()))
		}
		if g.shutdown != nil {
			g.closeErr = errors.Join(g.closeErr, g.shutdown(context.Background()))
		}
		close(g.closed)
	})
}

func (m *Manager) pinOwned(ctx context.Context) (context.Context, *generation, func(), error) {
	if ctx == nil {
		return ctx, nil, nil, fmt.Errorf("application context is required")
	}
	key := generationKey{m}
	if current, ok := ctx.Value(key).(*generation); ok {
		if ctx.Value(generationOwnerKey{m}) == current {
			return ctx, current, func() {}, nil
		}
		if current.acquire() {
			var once sync.Once
			return context.WithValue(ctx, generationOwnerKey{m}, current), current, func() { once.Do(current.release) }, nil
		}
	}
	m.publication.Lock()
	defer m.publication.Unlock()
	if m.stopped.Load() {
		return ctx, nil, nil, ErrClosed
	}
	current := m.active.Load()
	if current == nil {
		return ctx, nil, nil, fmt.Errorf("application has no published generation")
	}
	if !current.acquire() {
		return ctx, nil, nil, ErrStale
	}
	var once sync.Once
	release := func() { once.Do(current.release) }
	ctx = context.WithValue(ctx, key, current)
	ctx = context.WithValue(ctx, generationOwnerKey{m}, current)
	return ctx, current, release, nil
}

type indexedRuntimeLoader struct{ lease *bootstrapindex.Lease }

func (l indexedRuntimeLoader) LoadComponent(ctx context.Context, key spec.Key) (*registry.RegisteredComponent, error) {
	loaded, err := l.lease.LoadComponent(ctx, key)
	if err != nil {
		return nil, err
	}
	return loaded.Registration, nil
}

func (l indexedRuntimeLoader) LoadComponents(ctx context.Context, key spec.Key) ([]*registry.RegisteredComponent, error) {
	loaded, err := l.lease.LoadComponent(ctx, key)
	if err != nil {
		return nil, err
	}
	return append([]*registry.RegisteredComponent{loaded.Registration}, loaded.Related...), nil
}

func (l indexedRuntimeLoader) ResolveComponentRoute(method, path string) (spec.Key, *spec.Route, bool) {
	if l.lease == nil || l.lease.Snapshot() == nil {
		return spec.Key{}, nil, false
	}
	entry, route, _, ok := l.lease.Snapshot().Route(method, path)
	if !ok || entry == nil {
		return spec.Key{}, nil, false
	}
	return entry.Key(), route, true
}

type generationKey struct{ manager *Manager }
type generationOwnerKey struct{ manager *Manager }

func (m *Manager) pin(ctx context.Context) (context.Context, *generation, error) {
	if m == nil || ctx == nil {
		return ctx, nil, fmt.Errorf("application context is required")
	}
	if m.stopped.Load() {
		return ctx, nil, ErrClosed
	}
	key := generationKey{m}
	if current, ok := ctx.Value(key).(*generation); ok {
		return ctx, current, nil
	}
	current := m.active.Load()
	if current == nil {
		return ctx, nil, fmt.Errorf("application has no published generation")
	}
	return context.WithValue(ctx, key, current), current, nil
}

// Pin implements MCP's request snapshot source. It never loads a second
// generation when an outer HTTP or application invocation already pinned one.
func (m *Manager) Pin(ctx context.Context) (context.Context, mcpserver.ServerService, error) {
	ctx, current, err := m.pin(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if current.index != nil && ctx.Value(generationOwnerKey{m}) != current {
		m.publication.Lock()
		if !current.acquire() {
			m.publication.Unlock()
			return ctx, nil, ErrStale
		}
		m.nextExternalPin++
		pinID := m.nextExternalPin
		var once sync.Once
		release := func() {
			once.Do(func() {
				current.release()
				m.publication.Lock()
				delete(m.externalPins, pinID)
				m.publication.Unlock()
			})
		}
		if m.externalPins == nil {
			m.externalPins = map[uint64]func(){}
		}
		m.externalPins[pinID] = release
		m.publication.Unlock()
		ctx = context.WithValue(ctx, generationOwnerKey{m}, current)
		ctx = mcpserver.WithRelease(ctx, release)
	}
	return ctx, current.mcp, nil
}

// PinSnapshot supplies MCP request routing without creating a caller-retained
// public pin; the protocol handler acquires and releases operation ownership.
func (m *Manager) PinSnapshot(ctx context.Context) (context.Context, mcpserver.ServerService, error) {
	ctx, current, err := m.pin(ctx)
	if err != nil {
		return ctx, nil, err
	}
	return ctx, current.mcp, nil
}

// PinOwned is used by the MCP transport, which releases the generation after
// completing one protocol operation.
func (m *Manager) PinOwned(ctx context.Context) (context.Context, mcpserver.ServerService, error) {
	ctx, current, release, err := m.pinOwned(ctx)
	if err != nil {
		return ctx, nil, err
	}
	return mcpserver.WithRelease(ctx, release), current.mcp, nil
}

func (m *Manager) InvokeComponent(ctx context.Context, request exec.ComponentRequest) (any, error) {
	ctx, current, release, err := m.pinOwned(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return current.runtime.InvokeComponent(ctx, request)
}

// Types returns a detached view of the currently published type authority.
func (m *Manager) Types(ctx context.Context) (*typecatalog.Catalog, error) {
	_, current, release, err := m.pinOwned(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return current.types.Clone()
}
