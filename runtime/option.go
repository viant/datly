package runtime

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/runtime/route"
)

// Option configures application-scoped runtime dependencies.
type Option func(*options) error

type options struct {
	managedObservability    *Observability
	observabilityConfigured bool
	observability           ObservabilityConfig
	injector                []bindly.InjectorOption
	exposure                *route.Exposure
	resources               *resource.Store
	resourceFiles           bool
}

// WithExposedPackages restricts public HTTP/MCP endpoints by package import path.
// All supplied components remain registered for internal dependency calls.
func WithExposedPackages(include, exclude []string) Option {
	return func(options *options) error {
		var err error
		options.exposure, err = route.NewExposure(include, exclude)
		return err
	}
}

// WithResources installs the shared package resource store used by binding,
// transcription, artifact compilation, and runtime handlers.
func WithResources(resources *resource.Store) Option {
	return func(options *options) error {
		if resources == nil {
			return fmt.Errorf("resource store is required")
		}
		if options.resourceFiles {
			return fmt.Errorf("runtime WithResources cannot be combined with WithResourceFS; register files in the shared store before compilation")
		}
		if options.resources != nil && options.resources != resources {
			return fmt.Errorf("runtime resource stores conflict")
		}
		if options.resources == resources {
			return nil
		}
		options.resources = resources
		options.injector = append(options.injector, bindly.WithResources(resources))
		return nil
	}
}

// WithResourceFS registers a package filesystem for SQL, StructQL, codecs, and
// other compiled resources in a newly created store. It cannot be combined with
// WithResources: register files in that explicit store before compilation instead.
// embed.FS implements fs.FS and is retained unchanged.
func WithResourceFS(name string, source fs.FS) Option {
	return func(options *options) error {
		if source == nil {
			return fmt.Errorf("resource filesystem is required")
		}
		if options.resources != nil {
			return fmt.Errorf("runtime WithResourceFS cannot be combined with WithResources; register files in the shared store before compilation")
		}
		options.resourceFiles = true
		options.injector = append(options.injector, bindly.WithResourceFS(strings.TrimSpace(name), source))
		return nil
	}
}
