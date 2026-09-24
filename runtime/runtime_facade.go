package runtime

import (
	"context"
	"fmt"
	"github.com/viant/datly/observability"
	"io/fs"
	"sync"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/resource"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/provider/clients"
	"github.com/viant/datly/runtime/output"
	rregistry "github.com/viant/datly/runtime/registry"
	remotecore "github.com/viant/datly/runtime/remote"
	rroute "github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	xexec "github.com/viant/xdatly/exec"
)

type RegisteredComponent = rregistry.RegisteredComponent

type Runtime struct {
	ownsObservability  bool
	observability      *Observability
	bundle             *rroute.Bundle
	publicBundle       *rroute.Bundle
	registered         map[string]*RegisteredComponent
	metadata           map[string]*spec.Component
	loader             ComponentLoader
	exposure           *rroute.Exposure
	relatedExposure    sync.Map
	relatedMetadata    sync.Map
	canonicalConstants map[string]locator.Provider
	invoker            *handlerengine.Engine
	injector           *bindly.Injector
	// clientProviders are applied to every component that does not register
	// the same kind itself. clients is the default registry the runtime
	// created when no providers were configured; it is closed on Shutdown.
	clientProviders  []locator.Provider
	defaultProviders []locator.Provider
	clients          *clients.Registry
	ownsClients      bool
}

// ComponentLoader materializes one indexed component inside the currently
// pinned application generation.
type ComponentLoader interface {
	LoadComponent(context.Context, spec.Key) (*RegisteredComponent, error)
}

// Resources returns the shared package resource store used by this runtime.
func (r *Runtime) Resources() *resource.Store {
	if r == nil || r.injector == nil {
		return nil
	}
	return r.injector.Resources()
}

func NewRuntime(components []*RegisteredComponent, runtimeOptions ...Option) (*Runtime, error) {
	options := &options{}
	for _, configure := range runtimeOptions {
		if configure != nil {
			if err := configure(options); err != nil {
				return nil, err
			}
		}
	}
	observation := options.managedObservability
	if observation == nil {
		observation = &Observability{Recorder: observability.NewRecorder(options.observability.Logger, observability.WithReadingData(options.observability.ReadingData))}
	}
	// Outbound client capabilities are composed here, once, for every
	// component. Without explicit providers the runtime owns the default
	// registry and releases it on Shutdown; handlers only ever borrow.
	clientProviders := options.clientProviders
	var ownedClients *clients.Registry
	if !options.clientProvidersConfigured {
		ownedClients = clients.New()
		clientProviders = ownedClients.Providers()
	}
	remoteMapper := options.remoteMapper
	if remoteMapper == nil {
		remoteMapper = remotecore.NewMapper()
	}
	defaultProviders := append(append([]locator.Provider(nil), clientProviders...), handlerprovider.Static(rhandler.RemoteMapperCapabilityKey, remoteMapper))
	specs := make([]*spec.Component, 0, len(components))
	registered := make(map[string]*RegisteredComponent, len(components))
	metadata := make(map[string]*spec.Component, len(components))
	canonicalConstants := make(map[string]locator.Provider, len(components))
	for _, component := range components {
		if component == nil || component.Component == nil {
			continue
		}
		if err := component.Component.Settings.ValidateSequenceStrategy(); err != nil {
			return nil, err
		}
		if component.Input == nil {
			return nil, fmt.Errorf("component %s input contract is required", component.Component.Key.String())
		}
		for _, route := range component.Component.Routes {
			if route == nil {
				continue
			}
			if _, ok := component.Input.ForRoute(spec.RouteRef{Method: route.Method, Path: route.Path}); !ok {
				return nil, fmt.Errorf("component %s route input contract is missing for %s %s", component.Component.Key.String(), route.Method, route.Path)
			}
		}
		specs = append(specs, component.Component)
		entry := *component
		if reader, ok := entry.Reader.(observedReader); ok {
			entry.Reader = reader.WithRecorder(observation.Recorder)
		}
		if entry.Output == nil {
			var outputErr error
			entry.Output, outputErr = (output.Compiler{}).Compile(output.CompileInput{Component: component.Component, Type: component.OutputType})
			if outputErr != nil {
				return nil, fmt.Errorf("component %s output: %w", component.Component.Key.String(), outputErr)
			}
		}
		entry.Providers = withDefaultClientProviders(append([]locator.Provider(nil), component.Providers...), defaultProviders)
		constants, constantsErr := canonicalConstantValues(component.Component)
		if constantsErr != nil {
			return nil, fmt.Errorf("component %s constants: %w", component.Component.Key.String(), constantsErr)
		}
		if len(constants) > 0 {
			provider, providerErr := handlerprovider.Constants(constants)
			if providerErr != nil {
				return nil, fmt.Errorf("component %s constants: %w", component.Component.Key.String(), providerErr)
			}
			canonicalConstants[component.Component.Key.String()] = provider
		}
		registered[component.Component.Key.String()] = &entry
		metadata[component.Component.Key.String()] = component.Component
	}
	bundle, err := rroute.NewBundle(specs)
	if err != nil {
		return nil, err
	}
	publicBundle, err := publicRouteBundle(specs, options.exposure)
	if err != nil {
		return nil, err
	}
	injector, err := bindly.NewInjector(options.injector...)
	if err != nil {
		return nil, err
	}
	for _, entry := range registered {
		if err := bindStaticHandler(context.Background(), injector, entry); err != nil {
			return nil, fmt.Errorf("component %s handler: %w", entry.Component.Key.String(), err)
		}
	}
	if options.managedObservability == nil {
		if err := observation.initialize(options.observability); err != nil {
			return nil, err
		}
	}
	return &Runtime{
		observability: observation, ownsObservability: options.managedObservability == nil,
		bundle: bundle, publicBundle: publicBundle, exposure: options.exposure, registered: registered, canonicalConstants: canonicalConstants,
		metadata: metadata,
		invoker:  handlerengine.New(), injector: injector,
		clientProviders: clientProviders, defaultProviders: defaultProviders, clients: ownedClients, ownsClients: ownedClients != nil,
	}, nil
}

// NewIndexedRuntime publishes route metadata immediately and resolves each
// executable registration through loader on first invocation.
func NewIndexedRuntime(components []*spec.Component, preloaded []*RegisteredComponent, loader ComponentLoader, runtimeOptions ...Option) (*Runtime, error) {
	if loader == nil {
		return nil, fmt.Errorf("indexed runtime component loader is required")
	}
	r, err := NewRuntime(preloaded, runtimeOptions...)
	if err != nil {
		return nil, err
	}
	options := &options{}
	for _, configure := range runtimeOptions {
		if configure != nil {
			if err := configure(options); err != nil {
				return nil, err
			}
		}
	}
	specs := make([]*spec.Component, 0, len(components))
	r.metadata = make(map[string]*spec.Component, len(components))
	for _, component := range components {
		if component == nil {
			return nil, fmt.Errorf("indexed runtime component metadata is required")
		}
		clone := component.Clone()
		identity := clone.Key.String()
		if r.metadata[identity] != nil {
			return nil, fmt.Errorf("duplicate indexed runtime component %s", identity)
		}
		r.metadata[identity] = clone
		specs = append(specs, clone)
	}
	for _, component := range preloaded {
		if component == nil || component.Component == nil || r.metadata[component.Component.Key.String()] != nil {
			continue
		}
		clone := component.Component.Clone()
		r.metadata[clone.Key.String()] = clone
		specs = append(specs, clone)
	}
	r.bundle, err = rroute.NewBundle(specs)
	if err != nil {
		return nil, err
	}
	r.publicBundle, err = publicRouteBundle(specs, options.exposure)
	if err != nil {
		return nil, err
	}
	r.loader = loader
	r.exposure = options.exposure
	return r, nil
}

func publicRouteBundle(components []*spec.Component, exposure *rroute.Exposure) (*rroute.Bundle, error) {
	publicSpecs := make([]*spec.Component, 0, len(components))
	for _, component := range components {
		if component == nil {
			continue
		}
		if exposure != nil && !exposure.Allows(component.Key.Scope) {
			continue
		}
		clone := component.Clone()
		clone.Routes = clone.Routes[:0]
		for _, route := range component.Routes {
			if spec.PublicRoute(route) {
				clone.Routes = append(clone.Routes, route.Clone())
			}
		}
		if len(clone.Routes) > 0 {
			publicSpecs = append(publicSpecs, clone)
		}
	}
	return rroute.NewBundle(publicSpecs)
}

func (r *Runtime) registeredComponent(ctx context.Context, key spec.Key) (*RegisteredComponent, error) {
	if r == nil {
		return nil, fmt.Errorf("runtime is not configured")
	}
	if registered := r.registered[key.String()]; registered != nil {
		return registered, nil
	}
	if r.loader == nil {
		return nil, fmt.Errorf("registered component not found: %s", key.String())
	}
	registered, err := r.loader.LoadComponent(ctx, key)
	if err != nil {
		return nil, err
	}
	if registered == nil || registered.Component == nil || registered.Component.Key != key {
		return nil, fmt.Errorf("loaded component does not match indexed component %s", key.String())
	}
	return r.prepareLoadedComponent(registered)
}

// prepareLoadedComponent gives every lazy registration the same static
// capabilities as an eagerly registered component without mutating its loader.
func (r *Runtime) prepareLoadedComponent(registered *RegisteredComponent) (*RegisteredComponent, error) {
	if registered == nil || registered.Component == nil {
		return nil, fmt.Errorf("loaded component is required")
	}
	entry := *registered
	entry.Providers = withDefaultClientProviders(append([]locator.Provider(nil), registered.Providers...), r.defaultProviders)
	// Static dependencies are runtime-scoped, not request-scoped; a canceled
	// first lookup must not permanently poison this handler's one-time bind.
	if err := bindStaticHandler(context.Background(), r.injector, &entry); err != nil {
		return nil, fmt.Errorf("component %s handler: %w", entry.Component.Key.String(), err)
	}
	return &entry, nil
}

func bindStaticHandler(ctx context.Context, injector *bindly.Injector, entry *RegisteredComponent) error {
	if entry == nil || entry.Handler == nil {
		return nil
	}
	binder, ok := entry.Handler.(interface {
		BindStatic(context.Context, *bindly.Injector) error
	})
	if !ok {
		return nil
	}
	scope, err := injector.ForScope(entry.Providers...)
	if err != nil {
		return err
	}
	return binder.BindStatic(ctx, scope)
}

// LoadComponent exposes generation-scoped lazy resolution to protocol owners.
func (r *Runtime) LoadComponent(ctx context.Context, key spec.Key) (*RegisteredComponent, error) {
	return r.registeredComponent(ctx, key)
}

func (r *Runtime) LoadComponents(ctx context.Context, key spec.Key) ([]*RegisteredComponent, error) {
	if family, ok := r.loader.(interface {
		LoadComponents(context.Context, spec.Key) ([]*RegisteredComponent, error)
	}); ok {
		components, err := family.LoadComponents(ctx, key)
		if err != nil {
			return nil, err
		}
		prepared := make([]*RegisteredComponent, len(components))
		for index, component := range components {
			if component == nil {
				continue
			}
			ready, prepareErr := r.prepareLoadedComponent(component)
			if prepareErr != nil {
				return nil, prepareErr
			}
			prepared[index] = ready
			if r.ExposesComponent(key) && ready.Component.Key != key {
				r.relatedExposure.Store(component.Component.Key.String(), true)
				r.relatedMetadata.Store(component.Component.Key.String(), component.Component.Clone())
			}
		}
		return prepared, nil
	}
	component, err := r.registeredComponent(ctx, key)
	if err != nil {
		return nil, err
	}
	return []*RegisteredComponent{component}, nil
}

func (r *Runtime) ResolveComponentRoute(method, path string) (spec.Key, *spec.Route, bool) {
	if resolver, ok := r.loader.(interface {
		ResolveComponentRoute(method, path string) (spec.Key, *spec.Route, bool)
	}); ok {
		return resolver.ResolveComponentRoute(method, path)
	}
	return spec.Key{}, nil, false
}

// ResourceFS returns the original package filesystem registered under name.
func (r *Runtime) ResourceFS(name string) (fs.FS, bool) {
	if r == nil || r.injector == nil {
		return nil, false
	}
	return r.injector.ResourceFS(name)
}

// ReadResource reads a default or namespace-qualified package resource.
func (r *Runtime) ReadResource(reference string) ([]byte, error) {
	if r == nil || r.injector == nil {
		return nil, fmt.Errorf("runtime injector is not configured")
	}
	return r.injector.ReadResource(reference)
}

func (r *Runtime) RouteByMethodPath(method, path string) (*spec.Route, bool) {
	if r == nil || r.bundle == nil {
		return nil, false
	}
	if _, _, ok := r.publicComponentByRoute(method, path); !ok {
		return nil, false
	}
	return r.bundle.RouteByMethodPath(method, path)
}

// ComponentTargetByMethodPath resolves the exact public component target used
// by protocol authorization without exposing the mutable route bundle.
func (r *Runtime) ComponentTargetByMethodPath(method, path string) (dexec.ComponentTarget, bool) {
	if r == nil || r.bundle == nil {
		return dexec.ComponentTarget{}, false
	}
	component, _, ok := r.publicComponentByRoute(method, path)
	if !ok || component == nil {
		return dexec.ComponentTarget{}, false
	}
	route, ok := r.bundle.RouteByMethodPath(method, path)
	if !ok || route == nil {
		return dexec.ComponentTarget{}, false
	}
	return dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: route.Method, Path: route.Path}}, true
}

func (r *Runtime) AllowedMethodsForPath(path string) []string {
	if r == nil || r.bundle == nil {
		return nil
	}
	var result []string
	for _, method := range r.bundle.AllowedMethodsForPath(path) {
		if _, _, ok := r.publicComponentByRoute(method, path); ok {
			result = append(result, method)
		}
	}
	return result
}

// MatchPathParams returns router-derived path values for a protocol adapter.
func (r *Runtime) MatchPathParams(method, path string) (map[string]string, bool) {
	if r == nil || r.bundle == nil {
		return nil, false
	}
	_, pathParams, ok := r.publicComponentByRoute(method, path)
	return pathParams, ok
}

// ExecuteRoute resolves a component and sends its prepared invocation through
// the single handler engine using providers assembled by the protocol adapter.
func (r *Runtime) ExecuteRoute(ctx context.Context, method, path string, scope dexec.ProviderScope) (any, error) {
	if r != nil {
		actual, err := r.executeRoute(ctx, method, path, scope)
		if execCtx := xexec.GetContext(ctx); execCtx != nil {
			if err != nil {
				if execCtx.StatusCode == 0 {
					execCtx.StatusCode = 500
				}
				if execCtx.Status == "" {
					execCtx.Status = "error"
				}
				execCtx.SetError(err)
			} else if execCtx.StatusCode == 0 {
				execCtx.StatusCode = 200
				if execCtx.Status == "" {
					execCtx.Status = "ok"
				}
			}
		}
		return actual, err
	}
	if execCtx := xexec.GetContext(ctx); execCtx != nil {
		execCtx.StatusCode = 500
		execCtx.Status = "error"
	}
	return nil, fmt.Errorf("runtime is not configured")
}
