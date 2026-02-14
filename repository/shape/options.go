package shape

// Options stores shape facade dependencies and behavior flags.
type Options struct {
	Mode     Mode
	Strict   bool
	Name     string
	Scanner  Scanner
	Planner  Planner
	Loader   Loader
	Compiler DQLCompiler
	Runtime  RuntimeRegistrar
}

// Option mutates Options.
type Option func(*Options)

// NewOptions builds Options from varargs.
func NewOptions(opts ...Option) *Options {
	ret := &Options{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func WithMode(mode Mode) Option {
	return func(o *Options) {
		o.Mode = mode
	}
}

func WithStrict(strict bool) Option {
	return func(o *Options) {
		o.Strict = strict
	}
}

func WithName(name string) Option {
	return func(o *Options) {
		o.Name = name
	}
}

func WithScanner(scanner Scanner) Option {
	return func(o *Options) {
		o.Scanner = scanner
	}
}

func WithPlanner(planner Planner) Option {
	return func(o *Options) {
		o.Planner = planner
	}
}

func WithLoader(loader Loader) Option {
	return func(o *Options) {
		o.Loader = loader
	}
}

func WithCompiler(compiler DQLCompiler) Option {
	return func(o *Options) {
		o.Compiler = compiler
	}
}

func WithRuntime(runtime RuntimeRegistrar) Option {
	return func(o *Options) {
		o.Runtime = runtime
	}
}
