package application

import "github.com/viant/datly/runtime"

// Option configures application-owned services shared by managed generations.
type Option func(*options) error

type options struct {
	async         *AsyncConfig
	observability runtime.ObservabilityConfig
}

// WithAsync installs durable job services and starts the configured watcher on
// the first successful Reload. Configuration is fixed for this Manager lifetime.
func WithAsync(config AsyncConfig) Option {
	return func(options *options) error {
		copy := config
		options.async = &copy
		return nil
	}
}
