package report

import (
	"context"
	"fmt"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
)

// RuntimeCapabilities contains only the concrete runtime attachments that
// project composition permits an application to supply.
type RuntimeCapabilities struct {
	Handler    handler.Handler
	Reader     exec.Reader
	Invocation handler.InvocationCapabilities
	Providers  []locator.Provider
	DataSource exec.DataSource
}

// RuntimeConfigurator resolves concrete runtime capabilities from a compiled
// artifact without receiving the canonical registration carrier.
type RuntimeConfigurator interface {
	Configure(context.Context, *ComponentArtifact) (RuntimeCapabilities, error)
}

type RuntimeConfigureFunc func(context.Context, *ComponentArtifact) (RuntimeCapabilities, error)

func (f RuntimeConfigureFunc) Configure(ctx context.Context, artifact *ComponentArtifact) (RuntimeCapabilities, error) {
	if f == nil {
		return RuntimeCapabilities{}, fmt.Errorf("runtime configure function is required")
	}
	return f(ctx, artifact)
}

// RuntimeComponents builds one complete temporary registration set. Callers
// publish it only after this method succeeds.
func (c *Compilation) RuntimeComponents(ctx context.Context, configurator RuntimeConfigurator) ([]*registry.RegisteredComponent, error) {
	if c == nil {
		return nil, fmt.Errorf("report compilation is required")
	}
	result := make([]*registry.RegisteredComponent, 0, len(c.artifacts))
	for _, unit := range c.artifacts {
		if unit == nil || unit.artifact == nil || unit.artifact.Component == nil || unit.artifact.Input == nil {
			return nil, fmt.Errorf("compiled component artifact is incomplete")
		}
		capabilities := RuntimeCapabilities{}
		if configurator != nil {
			var err error
			capabilities, err = configurator.Configure(ctx, unit)
			if err != nil {
				return nil, fmt.Errorf("configure runtime component %s: %w", unit.artifact.Component.Key.String(), err)
			}
		}
		registered, err := unit.registration(capabilities)
		if err != nil {
			return nil, err
		}
		result = append(result, registered)
	}
	return result, nil
}

func (a *ComponentArtifact) registration(capabilities RuntimeCapabilities) (*registry.RegisteredComponent, error) {
	identity := a.artifact.Component.Key.String()
	if a.predefinedHandler != nil {
		if capabilities.Handler != nil || capabilities.Reader != nil {
			return nil, fmt.Errorf("runtime configurator replaced predefined report handler for %s", identity)
		}
		capabilities.Handler = a.predefinedHandler
	}
	if capabilities.Handler != nil && capabilities.Reader != nil {
		return nil, fmt.Errorf("runtime component %s cannot define both handler and reader", identity)
	}
	return a.artifact.Registration(registry.RegisteredComponent{
		Handler: capabilities.Handler, Reader: capabilities.Reader, Capabilities: capabilities.Invocation,
		Providers: append([]locator.Provider(nil), capabilities.Providers...), DataSource: capabilities.DataSource,
	})
}
