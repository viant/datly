package shape

import "context"

type (
	// Scanner discovers shape descriptors from Source.
	Scanner interface {
		Scan(ctx context.Context, source *Source, opts ...ScanOption) (*ScanResult, error)
	}

	// Planner normalizes discovered descriptors into execution plan.
	Planner interface {
		Plan(ctx context.Context, scan *ScanResult, opts ...PlanOption) (*PlanResult, error)
	}

	// Loader materializes runtime artifacts from normalized plan.
	Loader interface {
		LoadViews(ctx context.Context, plan *PlanResult, opts ...LoadOption) (*ViewArtifacts, error)
		LoadComponent(ctx context.Context, plan *PlanResult, opts ...LoadOption) (*ComponentArtifact, error)
	}

	// DQLCompiler compiles DQL source directly into a shape plan.
	DQLCompiler interface {
		Compile(ctx context.Context, source *Source, opts ...CompileOption) (*PlanResult, error)
	}

	// RuntimeRegistrar optionally registers loaded artifacts in runtime services.
	RuntimeRegistrar interface {
		RegisterViews(ctx context.Context, artifacts *ViewArtifacts) error
		RegisterComponent(ctx context.Context, artifacts *ComponentArtifact) error
	}

	ScanOptions    struct{}
	PlanOptions    struct{}
	LoadOptions    struct{}
	CompileOptions struct{}

	ScanOption    func(*ScanOptions)
	PlanOption    func(*PlanOptions)
	LoadOption    func(*LoadOptions)
	CompileOption func(*CompileOptions)
)

// Engine is a thin facade over scan -> plan -> load pipeline.
type Engine struct {
	options *Options
}

// New creates an Engine facade.
func New(opts ...Option) *Engine {
	return &Engine{options: NewOptions(opts...)}
}

// LoadViews is a package-level helper for struct source view loading.
func LoadViews(ctx context.Context, src any, opts ...Option) (*ViewArtifacts, error) {
	return New(opts...).LoadViews(ctx, src)
}

// LoadComponent is a package-level helper for struct source component loading.
func LoadComponent(ctx context.Context, src any, opts ...Option) (*ComponentArtifact, error) {
	return New(opts...).LoadComponent(ctx, src)
}

// LoadDQLViews is a package-level helper for DQL source view loading.
func LoadDQLViews(ctx context.Context, dql string, opts ...Option) (*ViewArtifacts, error) {
	return New(opts...).LoadDQLViews(ctx, dql)
}

// LoadDQLComponent is a package-level helper for DQL source component loading.
func LoadDQLComponent(ctx context.Context, dql string, opts ...Option) (*ComponentArtifact, error) {
	return New(opts...).LoadDQLComponent(ctx, dql)
}

// LoadViews executes scan -> plan -> load for struct source.
func (e *Engine) LoadViews(ctx context.Context, src any) (*ViewArtifacts, error) {
	source, err := e.structSource(src)
	if err != nil {
		return nil, err
	}
	plan, err := e.scanAndPlan(ctx, source)
	if err != nil {
		return nil, err
	}
	if e.options.Loader == nil {
		return nil, ErrLoaderNotConfigured
	}
	return e.options.Loader.LoadViews(ctx, plan)
}

// LoadComponent executes scan -> plan -> load for struct source.
func (e *Engine) LoadComponent(ctx context.Context, src any) (*ComponentArtifact, error) {
	source, err := e.structSource(src)
	if err != nil {
		return nil, err
	}
	plan, err := e.scanAndPlan(ctx, source)
	if err != nil {
		return nil, err
	}
	if e.options.Loader == nil {
		return nil, ErrLoaderNotConfigured
	}
	return e.options.Loader.LoadComponent(ctx, plan)
}

// LoadDQLViews executes compile -> load for DQL source.
func (e *Engine) LoadDQLViews(ctx context.Context, dql string) (*ViewArtifacts, error) {
	source, err := e.dqlSource(dql)
	if err != nil {
		return nil, err
	}
	plan, err := e.compile(ctx, source)
	if err != nil {
		return nil, err
	}
	if e.options.Loader == nil {
		return nil, ErrLoaderNotConfigured
	}
	return e.options.Loader.LoadViews(ctx, plan)
}

// LoadDQLComponent executes compile -> load for DQL source.
func (e *Engine) LoadDQLComponent(ctx context.Context, dql string) (*ComponentArtifact, error) {
	source, err := e.dqlSource(dql)
	if err != nil {
		return nil, err
	}
	plan, err := e.compile(ctx, source)
	if err != nil {
		return nil, err
	}
	if e.options.Loader == nil {
		return nil, ErrLoaderNotConfigured
	}
	return e.options.Loader.LoadComponent(ctx, plan)
}

func (e *Engine) compile(ctx context.Context, source *Source) (*PlanResult, error) {
	if e.options.Compiler == nil {
		return nil, ErrCompilerNotConfigured
	}
	return e.options.Compiler.Compile(ctx, source)
}

func (e *Engine) scanAndPlan(ctx context.Context, source *Source) (*PlanResult, error) {
	if e.options.Scanner == nil {
		return nil, ErrScannerNotConfigured
	}
	if e.options.Planner == nil {
		return nil, ErrPlannerNotConfigured
	}
	scanResult, err := e.options.Scanner.Scan(ctx, source)
	if err != nil {
		return nil, err
	}
	return e.options.Planner.Plan(ctx, scanResult)
}
