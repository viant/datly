package runtime

import (
	"context"
	"fmt"
	"github.com/viant/datly/observability"
	"io/fs"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/resource"
	dexec "github.com/viant/datly/exec"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/output"
	rregistry "github.com/viant/datly/runtime/registry"
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
	canonicalConstants map[string]locator.Provider
	invoker            *handlerengine.Engine
	injector           *bindly.Injector
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
	specs := make([]*spec.Component, 0, len(components))
	registered := make(map[string]*RegisteredComponent, len(components))
	canonicalConstants := make(map[string]locator.Provider, len(components))
	for _, component := range components {
		if component == nil || component.Component == nil {
			continue
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
		entry.Providers = append([]locator.Provider(nil), component.Providers...)
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
	}
	bundle, err := rroute.NewBundle(specs)
	if err != nil {
		return nil, err
	}
	publicBundle := bundle
	if options.exposure != nil {
		publicSpecs := make([]*spec.Component, 0, len(specs))
		for _, component := range specs {
			if options.exposure.Allows(component.Key.Scope) {
				publicSpecs = append(publicSpecs, component)
			}
		}
		publicBundle, err = rroute.NewBundle(publicSpecs)
		if err != nil {
			return nil, err
		}
	}
	injector, err := bindly.NewInjector(options.injector...)
	if err != nil {
		return nil, err
	}
	if options.managedObservability == nil {
		if err := observation.initialize(options.observability); err != nil {
			return nil, err
		}
	}
	return &Runtime{
		observability: observation, ownsObservability: options.managedObservability == nil,
		bundle: bundle, publicBundle: publicBundle, registered: registered, canonicalConstants: canonicalConstants,
		invoker: handlerengine.New(), injector: injector,
	}, nil
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
