package shape

// Options stores shape facade dependencies and behavior flags.
type Options struct {
	Mode                Mode
	Strict              bool
	Name                string
	Scanner             Scanner
	Planner             Planner
	Loader              Loader
	Compiler            DQLCompiler
	Runtime             RuntimeRegistrar
	CompileProfile      CompileProfile
	CompileMixedMode    CompileMixedMode
	UnknownNonReadMode  CompileUnknownNonReadMode
	ColumnDiscoveryMode CompileColumnDiscoveryMode
}

// Option mutates Options.
type Option func(*Options)

// NewOptions builds Options from varargs.
func NewOptions(opts ...Option) *Options {
	ret := &Options{
		CompileProfile:      CompileProfileCompat,
		CompileMixedMode:    CompileMixedModeExecWins,
		UnknownNonReadMode:  CompileUnknownNonReadWarn,
		ColumnDiscoveryMode: CompileColumnDiscoveryAuto,
	}
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

// WithCompileProfileDefault sets default compiler profile used by Engine DQL compile path.
func WithCompileProfileDefault(profile CompileProfile) Option {
	return func(o *Options) {
		o.CompileProfile = profile
	}
}

// WithMixedModeDefault sets default compiler mixed read/exec mode used by Engine DQL compile path.
func WithMixedModeDefault(mode CompileMixedMode) Option {
	return func(o *Options) {
		o.CompileMixedMode = mode
	}
}

// WithUnknownNonReadModeDefault sets default unknown non-read mode used by Engine DQL compile path.
func WithUnknownNonReadModeDefault(mode CompileUnknownNonReadMode) Option {
	return func(o *Options) {
		o.UnknownNonReadMode = mode
	}
}

// WithColumnDiscoveryModeDefault sets default column discovery policy used by Engine DQL compile path.
func WithColumnDiscoveryModeDefault(mode CompileColumnDiscoveryMode) Option {
	return func(o *Options) {
		o.ColumnDiscoveryMode = mode
	}
}

// WithLegacyTranslatorDefaults configures Engine compile defaults to legacy-compatible behavior.
func WithLegacyTranslatorDefaults() Option {
	return func(o *Options) {
		o.Strict = false
		o.CompileProfile = CompileProfileCompat
		o.CompileMixedMode = CompileMixedModeExecWins
		o.UnknownNonReadMode = CompileUnknownNonReadWarn
		o.ColumnDiscoveryMode = CompileColumnDiscoveryAuto
	}
}

func WithCompileStrict(strict bool) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.Strict = strict
	}
}

func WithMixedMode(mode CompileMixedMode) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.MixedMode = mode
	}
}

func WithUnknownNonReadMode(mode CompileUnknownNonReadMode) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.UnknownNonReadMode = mode
	}
}

func WithCompileProfile(profile CompileProfile) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.Profile = profile
	}
}

func WithColumnDiscoveryMode(mode CompileColumnDiscoveryMode) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.ColumnDiscoveryMode = mode
	}
}

// WithDQLPathMarker overrides the path marker used to locate platform root from source path.
// Default is "/dql/".
func WithDQLPathMarker(marker string) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.DQLPathMarker = marker
	}
}

// WithRoutesRelativePath overrides routes path relative to detected platform root.
// Default is "repo/dev/Datly/routes".
func WithRoutesRelativePath(path string) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.RoutesRelativePath = path
	}
}

// WithTypeContextPackageDir sets default type-context package directory (for xgen parity).
func WithTypeContextPackageDir(dir string) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.TypePackageDir = dir
	}
}

// WithTypeContextPackageName sets default type-context package name (for xgen parity).
func WithTypeContextPackageName(name string) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.TypePackageName = name
	}
}

// WithTypeContextPackagePath sets default type-context package import path (for xgen parity).
func WithTypeContextPackagePath(path string) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.TypePackagePath = path
	}
}

// WithTypeContextPackageDefaults sets package dir/name/path in one call.
func WithTypeContextPackageDefaults(dir, name, path string) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.TypePackageDir = dir
		o.TypePackageName = name
		o.TypePackagePath = path
	}
}

// WithInferTypeContextDefaults enables/disables source-path based type context defaults.
func WithInferTypeContextDefaults(enabled bool) CompileOption {
	return func(o *CompileOptions) {
		if o == nil {
			return
		}
		o.InferTypeContext = &enabled
	}
}
