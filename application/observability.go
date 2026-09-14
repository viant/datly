package application

import "github.com/viant/datly/runtime"

// WithObservability configures one capture/export owner for this Manager. It is
// stable across reloads and pinned generations, like the managed warmup lifetime.
func WithObservability(config runtime.ObservabilityConfig) Option {
	return func(options *options) error {
		copy := config
		if config.OTel != nil {
			export := *config.OTel
			copy.OTel = &export
		}
		options.observability = copy
		return nil
	}
}
