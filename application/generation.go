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
}

type Request struct {
	// Revision is the monotonic revision of the complete authored source set.
	Revision uint64
	// Compile receives a detached catalog. It must compile the complete source
	// set and all plans before returning, without side effects on active services.
	Compile func(context.Context, *typecatalog.Catalog) (*Build, error)
}

type generation struct {
	revision uint64
	types    *typecatalog.Catalog
	runtime  *runtime.Runtime
	http     *gateway.Handler
	mcp      *mcp.Service
	sources  map[*spec.Component]bool
	async    *asyncGeneration
}

type Manager struct {
	active       atomic.Pointer[generation]
	seed         *typecatalog.Catalog
	publication  sync.Mutex
	stopped      atomic.Bool
	warmups      *gateway.WarmupLifetime
	observation  *runtime.Observability
	async        *asyncLifetime
	shutdownDone chan struct{}
	shutdownErr  error
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
	sources := map[*spec.Component]bool{}
	components := make([]*registry.RegisteredComponent, 0, len(built.Components))
	for _, component := range built.Components {
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
	rt, err := runtime.NewRuntime(components, runtimeOptions...)
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
	httpHandler, err := m.newHTTPHandler(ctx, built, gateway.HandlerInput{Async: m.jobAdmission(async), Runtime: rt, Components: components, Logger: built.Logger, Version: built.Version})
	if err != nil {
		return err
	}
	next := &generation{async: async, revision: request.Revision, types: types, runtime: rt, http: httpHandler, mcp: service, sources: sources}
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
	published = true
	m.async.start()
	return nil
}

type generationKey struct{ manager *Manager }

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
	return ctx, current.mcp, nil
}

func (m *Manager) InvokeComponent(ctx context.Context, request exec.ComponentRequest) (any, error) {
	ctx, current, err := m.pin(ctx)
	if err != nil {
		return nil, err
	}
	return current.runtime.InvokeComponent(ctx, request)
}

// Types returns a detached view of the currently published type authority.
func (m *Manager) Types(ctx context.Context) (*typecatalog.Catalog, error) {
	_, current, err := m.pin(ctx)
	if err != nil {
		return nil, err
	}
	return current.types.Clone()
}
