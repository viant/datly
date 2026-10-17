package transcribe

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/viant/datly/bootstrap"
	routecompiler "github.com/viant/datly/bootstrap/routes"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	authoring "github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/handlergen"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/transcribe/internal/program"
	"github.com/viant/datly/typecatalog"
)

var ErrNilSource = errors.New("transcribe: nil source")

// Result is the compile-time product passed to later plan, type-resolution,
// generation, and persistence stages. Runtime artifacts are intentionally not
// part of this model.
type Result struct {
	Source                  *Source
	Component               *spec.Component
	PreparedSQL             string
	Statements              statement.Statements
	SourceMap               *SourceMap
	TypeContext             *typecatalog.ResolutionContext
	TypeResolver            *typecatalog.Resolver
	TypeAuthority           typecatalog.Authority
	Declarations            gen.Declarations
	Contracts               gen.ContractReferences
	Views                   gen.ViewReferences
	ViewBindings            gen.ViewBindings
	GeneratedTypes          []gen.GeneratedTypeReference
	GoHandler               *gen.GoHandlerAsset
	StatementProgram        string
	ExternalHandler         *gen.ExternalHandler
	VeltyHandler            *gen.VeltyHandlerAsset
	ContractTypeOverrides   ContractTypeOverrides
	AuthoredBorrowedSQLRows []spec.BorrowedSQLRow
	borrowedConsumedFiles   []gen.BorrowedAuthorityFile
	// generatedOutputRows retains exact SQL-owned output admission for the next linking pass.
	generatedOutputRows map[*spec.View]bool
	Diagnostics         []*Diagnostic
}

// ContractTypeOverrides records only type names explicitly authored by DQL.
// Package-inherited settings are intentionally excluded so ownership decisions
// never need to infer provenance from equal strings.
type ContractTypeOverrides = handlergen.ContractTypeOverrides

// Compiler orchestrates source preparation, metadata linking, read planning,
// column discovery, and handler compilation.
type Compiler struct {
	comparisonOnly    bool
	factoryOwner      *factoryOwnerReference
	factoryReferences []*factoryOwnerReference
}

func NewCompiler() *Compiler {
	return &Compiler{}
}

func (c *Compiler) Compile(ctx context.Context, source *Source) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, ErrNilSource
	}
	// Source-owner evidence belongs to this compilation; reusable compilers must
	// not attach a previous source's closure to another generated component.
	c.factoryReferences = nil
	if header, body, err := dql.ParseHandlerSource(source.Text); err != nil {
		return nil, err
	} else if header != nil {
		if header.Name == "" {
			header.Name = source.Name
		}
		if header.Factory != "" {
			return c.compileSourceHandler(ctx, source, header, body)
		}
		return c.compileHandler(source, header, body)
	}
	compilation, err := prepareCompilation(ctx, source)
	if err != nil {
		return nil, err
	}
	result := compilation.result
	if result.Component.Static != nil {
		if source.PackageComponent != nil || source.GoHandler != nil || source.VeltyHandler != nil {
			return nil, fmt.Errorf("static source cannot overlay an executable component")
		}
		result.TypeContext = compileTypeContext(result.Source, result.Component.TypeContext)
		return result, nil
	}
	if err := compilation.loadMetadata(c.comparisonOnly); err != nil {
		return nil, err
	}
	if err := compilation.compileViews(ctx); err != nil {
		return nil, err
	}
	if err := compilation.refine(ctx); err != nil {
		return nil, err
	}
	if err := compilation.compileHandler(); err != nil {
		return nil, err
	}
	if err := compilation.linkTypes(); err != nil {
		return nil, err
	}
	if err := compilation.authoredInputs.validate(ctx); err != nil {
		return nil, err
	}
	if err := c.admitBorrowedSQLRows(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

// componentCompilation holds only the products shared by successive compiler
// stages. Public Result remains the boundary used by planning and generation.
type componentCompilation struct {
	result         *Result
	authored       *spec.Component
	prepared       *dql.PreparedSource
	rows           *generatedRowAuthority
	authoredInputs *authoredInputProof
}

func prepareCompilation(ctx context.Context, source *Source) (*componentCompilation, error) {
	preparation, err := prepareSourceCompilation(ctx, source)
	if err != nil {
		return nil, err
	}
	source = preparation.apply(source)
	prepared, sourceMap, readerName := preparation.prepared, preparation.sourceMap, preparation.readerName
	component, err := dql.ParsePreparedComponentSource(source.Scope, source.Name, prepared)
	// A Go-only component has package authority and no DQL overlay. Keep it
	// on the same load/compile path without inventing an authored route.
	if errors.Is(err, dql.ErrMissingRouteDirective) && source.PackageComponent != nil && strings.TrimSpace(source.Text) == "" {
		component = &spec.Component{Key: source.PackageComponent.Key, Name: source.PackageComponent.Name}
		err = nil
	}
	if err != nil {
		message := err.Error()
		if source.Path != "" {
			message = fmt.Sprintf("failed to compile %q: %s", source.Path, message)
		}
		diagnostic := &Diagnostic{
			Code:     dql.DiagnosticParse,
			Severity: SeverityError,
			Message:  message,
			Path:     source.Path,
			Span:     pointSpan(source.Text, 0),
		}
		if item, ok := dql.DiagnosticForError(err); ok {
			diagnostic.Code = item.Code
			diagnostic.Span = Span{Start: sourceMap.Position(item.Offset), End: sourceMap.Position(item.End)}
		} else {
			sourceMap.Remap([]*Diagnostic{diagnostic})
		}
		return nil, &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
	}
	if readerName != "" {
		for _, route := range component.Routes {
			if route != nil {
				route.Name = readerName
			}
		}
	}
	return &componentCompilation{
		result:   &Result{Source: source, Component: component, SourceMap: sourceMap},
		authored: component, prepared: prepared,
	}, nil
}

func (c *componentCompilation) loadMetadata(comparisonOnly bool) error {
	source, component, prepared := c.result.Source, c.authored, c.prepared
	var err error
	contractTypeOverrides := authoredContractTypeOverrides(component)
	authoredBorrowedRows := authoredBorrowedSQLRows(component)
	var borrowedConsumed []gen.BorrowedAuthorityFile
	if len(authoredBorrowedRows) > 0 && !comparisonOnly && source.ColumnRefiner != nil {
		borrowedConsumed, err = sealBorrowedSourcePackage(source.BaseDir())
		if err != nil {
			return err
		}
		if err = validateBorrowedSourceText(source, borrowedConsumed); err != nil {
			return err
		}
		captured, err := capturedBorrowedResources(source, borrowedConsumed)
		if err != nil {
			return err
		}
		sourceCopy := *source
		sourceCopy.Resources = captured
		source = &sourceCopy
	}

	loader := &link.Loader{PackageComponent: source.PackageComponent, AuthoredComponent: component, DQLAuthored: strings.TrimSpace(source.Text) != ""}
	component, err = loader.Load()
	if err != nil {
		return err
	}
	if err = (routecompiler.Compiler{Component: component}).Compile(); err != nil {
		return err
	}
	if err = synthesizeConstantParams(component); err != nil {
		span := pointSpan(source.Text, 0)
		if authored, ok := constantDiagnosticSpan(prepared, err); ok {
			span = Span{Start: positionAt(source.Text, authored.Start), End: positionAt(source.Text, authored.End)}
		}
		diagnostic := &Diagnostic{Code: "DQL-CONST", Severity: SeverityError, Message: err.Error(), Path: source.Path, Span: span}
		return &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
	}
	compiledTypeContext := compileTypeContext(source, component.TypeContext)
	if err = compilePredicateContracts(component, source.Types, compiledTypeContext); err != nil {
		return err
	}
	var typeResolver *typecatalog.Resolver
	if source.Types != nil {
		typeResolver, err = typecatalog.NewResolver(source.Types, typecatalog.TranscribeAuthority, compiledTypeContext)
		if err != nil {
			return err
		}
	}
	if !source.Const.Empty() {
		constantsResolver := typeResolver
		if constantsResolver == nil {
			constantsResolver, err = typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, compiledTypeContext)
			if err != nil {
				return err
			}
		}
		if err = source.Const.Validate(component, constantsResolver.Type); err != nil {
			return err
		}
	}
	c.result.Source, c.result.Component = source, component
	c.result.TypeContext, c.result.TypeResolver = compiledTypeContext, typeResolver
	c.result.TypeAuthority = typecatalog.TranscribeAuthority
	c.result.ContractTypeOverrides = contractTypeOverrides
	c.result.AuthoredBorrowedSQLRows, c.result.borrowedConsumedFiles = authoredBorrowedRows, borrowedConsumed
	c.result.PreparedSQL, c.result.Statements = prepared.SQL, prepared.Statements
	return nil
}

func (c *componentCompilation) compileViews(ctx context.Context) error {
	source, component, prepared := c.result.Source, c.result.Component, c.prepared
	sourceCopy := *source
	source = &sourceCopy
	c.result.Source = source
	var proofErr error
	source.selfGeneratedInputViews, source.inheritedGeneratedCurrents, proofErr = generatedCurrentAuthority(ctx, source, component, c.authored, c.result.TypeResolver)
	if proofErr != nil {
		return proofErr
	}
	sourceMap, compiledTypeContext, typeResolver := c.result.SourceMap, c.result.TypeContext, c.result.TypeResolver
	declarationFailure := func(err error) error {
		span := pointSpan(source.Text, 0)
		var declarationErr *declarationError
		if errors.As(err, &declarationErr) && prepared.Directives != nil {
			if authored, ok := prepared.Directives.ParamSpans[declarationErr.identity]; ok {
				span = Span{Start: positionAt(source.Text, authored.Start), End: positionAt(source.Text, authored.End)}
			}
		}
		diagnostic := &Diagnostic{
			Code: diagnosticDeclarationSQL, Severity: SeverityError, Message: err.Error(), Path: source.Path,
			Span: span,
		}
		return &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
	}
	metadataDeclarations, err := newDeclarationCompiler(component, c.authored.Views, source.Resources).compile()
	if err != nil {
		return declarationFailure(err)
	}
	generatedRows, err := collectGeneratedRowAuthority(component, prepared, metadataDeclarations, typeResolver, source.Path, sourceMap)
	if err != nil {
		return err
	}
	readPlan := generatedRows.root
	if generatedRows.plan == nil {
		// Without package declarations, retain ordinary catalog validation.
		readPlan, err = (&readPlanCompiler{sourceMap: sourceMap, path: source.Path, types: typeResolver}).compile(component, prepared)
		if err != nil {
			return err
		}
	}
	if readPlan != nil {
		component.RootView = readPlan
	}
	declarations := metadataDeclarations
	if declarations != nil {
		component.Views, err = declarations.removeSkeletons(component.Views)
		if err != nil {
			return err
		}
		component.Views, err = link.MergeViews(component.Views, declarations.views)
		if err != nil {
			return err
		}
		component.RootView, err = mergeOutputRelations(component.RootView, declarations.outputRelations)
		if err != nil {
			return err
		}
	}
	// Canonicalize selector targets proven by the transcribed graph and reject
	// ambiguity now. Nested views linked from a Go output type are assembled by
	// artifact bootstrap, so targets unknown here stay authored until that
	// completed graph validates them.
	if err := resolveQuerySelectorViews(component); err != nil {
		return err
	}
	if _, err := bootstrap.NormalizeCodecReferences(component, compiledTypeContext); err != nil {
		return err
	}
	var declaredViews map[string]*spec.View
	if declarations != nil {
		declaredViews = declarations.viewsByParam
	}
	viewBindings, err := link.NormalizeIndependentViewParams(component, declaredViews)
	if err != nil {
		return fmt.Errorf("normalize independent view contracts: %w", err)
	}
	packageViews, err := packageInputViewReferences(component, source.PackageComponent, typeResolver, source.selfGeneratedInputViews)
	if err != nil {
		return err
	}
	c.result.Declarations = declarations.generation
	c.result.ViewBindings, c.result.Views = gen.ViewBindings(viewBindings), packageViews
	authoredViews, proof, err := authoredInputViewReferences(ctx, c.result)
	if err != nil {
		return err
	}
	for identity, reference := range authoredViews {
		if c.result.Views == nil {
			c.result.Views = gen.ViewReferences{}
		}
		if prior := c.result.Views[identity]; prior != nil && prior.DescriptorKey != reference.DescriptorKey {
			return fmt.Errorf("independent view %q has conflicting authored ownership", identity)
		}
		c.result.Views[identity] = reference
	}
	c.authoredInputs = proof
	c.rows = generatedRows
	return nil
}

func (c *componentCompilation) refine(ctx context.Context) error {
	source, component, compiledTypeContext := c.result.Source, c.result.Component, c.result.TypeContext
	applySourceDefaults(component, source)
	if err := column.NormalizeIdentifierMetadata(component); err != nil {
		return err
	}
	if err := refineComponentColumns(ctx, component, source, c.result.Declarations, c.result.ViewBindings, c.result.TypeResolver, gen.Input{Views: c.result.Views}); err != nil {
		return err
	}

	if _, err := bootstrap.NormalizeCodecReferences(component, compiledTypeContext); err != nil {
		return err
	}
	if err := authoring.BackfillRelationMetadata(component, source.Resources); err != nil {
		return err
	}
	if err := authoring.BackfillReportMetadata(component, source.Resources); err != nil {
		return err
	}
	enrichDescription(ctx, component, source.Docs)
	return nil
}

func (c *componentCompilation) compileHandler() error {
	source, component, prepared, sourceMap := c.result.Source, c.result.Component, c.prepared, c.result.SourceMap
	goHandler, err := source.GoHandler.Clone()
	if err != nil {
		return err
	}
	veltyHandler := source.VeltyHandler.Clone()
	statementProgram, err := program.BufferedStatement(prepared, goHandler != nil || veltyHandler != nil)
	if err != nil {
		return err
	}
	if statementProgram == "" {
		veltyHandler, err = program.Resolve(prepared, component, goHandler, veltyHandler)
	}
	if err != nil {
		span := pointSpan(source.Text, 0)
		var programErr *program.Error
		if errors.As(err, &programErr) && programErr.End > programErr.Start {
			span = Span{
				Start: sourceMap.Position(programErr.Start),
				End:   sourceMap.Position(programErr.End),
			}
		}
		diagnostic := &Diagnostic{
			Code: "DQL-HANDLER", Severity: SeverityError, Message: err.Error(), Path: source.Path,
			Span: span,
		}
		return &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
	}
	c.result.GoHandler, c.result.VeltyHandler, c.result.StatementProgram = goHandler, veltyHandler, statementProgram
	return nil
}

func (c *componentCompilation) linkTypes() error {
	result := c.result
	if err := linkDeclaredOutputView(result, c.authored, c.rows); err != nil {
		return err
	}
	if err := linkDeclaredOutputContract(result, c.authored); err != nil {
		return err
	}
	if err := linkDeclaredRelatedViews(result, c.rows); err != nil {
		return err
	}
	return nil
}

func authoredContractTypeOverrides(component *spec.Component) ContractTypeOverrides {
	if component == nil || component.Settings == nil {
		return ContractTypeOverrides{}
	}
	return ContractTypeOverrides{
		Input:  strings.TrimSpace(component.Settings.InputType),
		Output: strings.TrimSpace(component.Settings.OutputType),
	}
}

func compileTypeContext(source *Source, authored *spec.TypeContext) *typecatalog.ResolutionContext {
	var scope, directory string
	if source != nil {
		scope, directory = source.Scope, source.BaseDir()
	}
	return link.TypeContext(scope, directory, authored)
}

func applySourceDefaults(component *spec.Component, source *Source) {
	if component == nil || source == nil || strings.TrimSpace(source.Connector) == "" {
		return
	}
	if component.Settings == nil {
		component.Settings = &spec.Settings{}
	}
	if strings.TrimSpace(component.Settings.DefaultConnector) == "" {
		component.Settings.DefaultConnector = strings.TrimSpace(source.Connector)
	}
}

// refineComponentColumns shares the reader discovery order with generated
// custom contracts: body columns first, then typed dependent input reads.
func refineComponentColumns(ctx context.Context, component *spec.Component, source *Source, declarations gen.Declarations, viewBindings gen.ViewBindings, typeResolver *typecatalog.Resolver, factoryInputs ...gen.Input) error {
	return refineComponentColumnsWithOwnerProof(ctx, component, source, declarations, viewBindings, typeResolver, nil, factoryInputs...)
}

func refineComponentColumnsWithOwnerProof(ctx context.Context, component *spec.Component, source *Source, declarations gen.Declarations, viewBindings gen.ViewBindings, typeResolver *typecatalog.Resolver, ownerProof func(*spec.Component) error, factoryInputs ...gen.Input) error {
	return refineComponentColumnsWithFactoryProof(ctx, component, source, declarations, viewBindings, typeResolver, ownerProof, nil, factoryInputs...)
}

func refineComponentColumnsWithFactoryProof(ctx context.Context, component *spec.Component, source *Source, declarations gen.Declarations, viewBindings gen.ViewBindings, typeResolver *typecatalog.Resolver, ownerProof func(*spec.Component) error, composition *authoring.FactoryComposition, factoryInputs ...gen.Input) error {
	var factoryShape *gen.ExternalHandler
	var linkedViews gen.ViewReferences
	if len(factoryInputs) != 0 {
		factoryShape = factoryInputs[0].ExternalHandler
		linkedViews = factoryInputs[0].Views
	}
	if source.ColumnRefiner != nil {
		if source.PackageComponent != nil && strings.TrimSpace(source.Text) == "" && source.LinkedOutputType != nil {
			if err := bootstrap.CompileOutputColumns(component, source.LinkedOutputType); err != nil {
				return err
			}
		}
		columnCompilation := source.ColumnRefiner.BeginCompilation()
		templateInput, compileErr := (&discoveryInputCompiler{
			component: component, declarations: declarations,
			viewBindings: viewBindings, resolver: typeResolver, source: source, factoryShape: factoryShape, views: linkedViews,
		}).compile()
		if compileErr != nil {
			return compileErr
		}
		rootDiscovery := component
		if source.LinkedHandler != nil && component.RootView != nil && component.RootView.Source != nil {
			rootSource := component.RootView.Source.Clone()
			if err := dsql.ResolveSource(component.RootView.Name, rootSource, source.Resources); err != nil {
				return err
			}
			classification := statement.Parse(rootSource.SQL).Classify()
			if classification.HasService && !classification.HasRead && !classification.HasUnknown {
				// A linked handler owns the buffered statement program. It has no
				// SQL result set to discover; independent input reads still do.
				rootDiscovery = component.Clone()
				rootDiscovery.RootView = nil
			}
		}
		rootDiscovery, transfer := composition.Discovery(rootDiscovery)
		if err := columnCompilation.RefineRoot(ctx, rootDiscovery, source.Resources, templateInput); err != nil {
			return err
		}
		transfer()
		if ownerProof != nil {
			if err := ownerProof(component); err != nil {
				return err
			}
		}
		templateInput, compileErr = (&discoveryInputCompiler{
			component: component, declarations: declarations,
			viewBindings: viewBindings, resolver: typeResolver, source: source, factoryShape: factoryShape, views: linkedViews,
		}).compile()
		if compileErr != nil {
			return compileErr
		}
		if err := columnCompilation.RefineViews(ctx, component, source.Resources, templateInput); err != nil {
			return err
		}
	} else if err := column.New(nil).ValidateSourceProjections(component, source.Resources, typeResolver); err != nil {
		return err
	}
	return nil
}
