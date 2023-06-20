package codegen

type (
	Options struct {
		pkg        string
		withInsert bool
		withUpdate bool
		withPatch  bool
		withDML    bool
	}

	Option func(o *Options)
)

func (o *Options) apply(opts []Option) {
	if len(opts) == 0 {
		return
	}
	for _, opt := range opts {
		opt(o)
	}
}

func WithPackage(pkg string) Option {
	return func(o *Options) {
		o.pkg = pkg
	}
}

func WithDML() Option {
	return func(o *Options) {
		o.withDML = true
	}
}

func WithInsert() Option {
	return func(o *Options) {
		o.withInsert = true
	}
}

func WithUpdate() Option {
	return func(o *Options) {
		o.withUpdate = true
	}
}

func WithPatch() Option {
	return func(o *Options) {
		o.withInsert = true
		o.withUpdate = true
		o.withPatch = true
	}
}
