package transcribe

import (
	"context"
	"errors"
	"fmt"
	routecompiler "github.com/viant/datly/bootstrap/routes"
	"path"
	"strings"

	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/dql/statement"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
)

var ErrNilSource = errors.New("transcribe: nil source")

// Result is the compile-time product passed to later plan, type-resolution,
// generation, and persistence stages. Runtime artifacts are intentionally not
// part of this model.
type Result struct {
	Source                *Source
	Component             *spec.Component
	PreparedSQL           string
	Statements            statement.Statements
	SourceMap             *SourceMap
	TypeContext           *typecatalog.ResolutionContext
	TypeResolver          *typecatalog.Resolver
	TypeAuthority         typecatalog.Authority
	Declarations          gen.Declarations
	Contracts             gen.ContractReferences
	Views                 gen.ViewReferences
	ViewBindings          gen.ViewBindings
	GeneratedTypes        []gen.GeneratedTypeReference
	GoHandler             *gen.GoHandlerAsset
	VeltyHandler          *gen.VeltyHandlerAsset
	ContractTypeOverrides ContractTypeOverrides
	Diagnostics           []*Diagnostic
}

// ContractTypeOverrides records only type names explicitly authored by DQL.
// Package-inherited settings are intentionally excluded so ownership decisions
// never need to infer provenance from equal strings.
type ContractTypeOverrides struct {
	Input  string
	Output string
}

// Compiler is the single authored-source orchestration entrypoint. Its body is
// intentionally narrow while the original preprocess and plan stages are
// ported behind this stable boundary.
type Compiler struct{}

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
	prepared := dql.PrepareSource(source.Text)
	sourceMap := newSourceMap(len(source.Text), nil, prepared.TrimPrefix, source.Text)
	if len(prepared.Diagnostics) > 0 {
		diagnostics := make([]*Diagnostic, 0, len(prepared.Diagnostics))
		for _, item := range prepared.Diagnostics {
			diagnostics = append(diagnostics, &Diagnostic{
				Code: item.Code, Severity: SeverityError, Message: item.Message, Path: source.Path,
				Span: Span{Start: positionAt(source.Text, item.Offset), End: positionAt(source.Text, item.End)},
			})
		}
		return nil, &CompileError{Cause: prepared.Err(), Diagnostics: diagnostics}
	}
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
			diagnostic.Span = Span{Start: positionAt(source.Text, item.Offset), End: positionAt(source.Text, item.End)}
		} else {
			sourceMap.Remap([]*Diagnostic{diagnostic})
		}
		return nil, &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
	}
	if component.Static != nil {
		if source.PackageComponent != nil || source.GoHandler != nil || source.VeltyHandler != nil {
			return nil, fmt.Errorf("static source cannot overlay an executable component")
		}
		return &Result{Source: source, Component: component, SourceMap: sourceMap, TypeContext: compileTypeContext(source, component.TypeContext)}, nil
	}
	authoredViews := component.Views
	contractTypeOverrides := authoredContractTypeOverrides(component)
	loader := &componentLoader{packageComponent: source.PackageComponent, authoredComponent: component}
	component, err = loader.Load()
	if err != nil {
		return nil, err
	}
	if err = (routecompiler.Compiler{Component: component}).Compile(); err != nil {
		return nil, err
	}
	if err = synthesizeConstantParams(component); err != nil {
		span := pointSpan(source.Text, 0)
		if authored, ok := constantDiagnosticSpan(prepared, err); ok {
			span = Span{Start: positionAt(source.Text, authored.Start), End: positionAt(source.Text, authored.End)}
		}
		diagnostic := &Diagnostic{Code: "DQL-CONST", Severity: SeverityError, Message: err.Error(), Path: source.Path, Span: span}
		return nil, &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
	}
	compiledTypeContext := compileTypeContext(source, component.TypeContext)
	if err = (readerpredicate.DefinitionCompiler{Context: compiledTypeContext}).Compile(component); err != nil {
		return nil, err
	}
	var typeResolver *typecatalog.Resolver
	if source.Types != nil {
		typeResolver, err = typecatalog.NewResolver(source.Types, typecatalog.TranscribeAuthority, compiledTypeContext)
		if err != nil {
			return nil, err
		}
	}
	if !source.Const.Empty() {
		constantsResolver := typeResolver
		if constantsResolver == nil {
			constantsResolver, err = typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.TranscribeAuthority, compiledTypeContext)
			if err != nil {
				return nil, err
			}
		}
		if err = source.Const.Validate(component, constantsResolver.Type); err != nil {
			return nil, err
		}
	}
	readPlan, err := (&readPlanCompiler{sourceMap: sourceMap, path: source.Path, types: typeResolver}).compile(component, prepared)
	if err != nil {
		return nil, err
	}
	if readPlan != nil {
		component.RootView = readPlan
	}
	declarations, err := newDeclarationCompiler(component, authoredViews).compile()
	if err != nil {
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
		return nil, &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
	}
	if declarations != nil {
		component.Views, err = declarations.removeSkeletons(component.Views)
		if err != nil {
			return nil, err
		}
		component.Views, err = loader.mergeViews(component.Views, declarations.views)
		if err != nil {
			return nil, err
		}
		component.RootView, err = mergeOutputRelations(component.RootView, declarations.outputRelations)
		if err != nil {
			return nil, err
		}
	}
	// A Go-only holder's nested view graph is assembled from linked output
	// types by artifact bootstrap, not by this source transcription stage.
	// Preserve its generated selector name until that typed graph is available.
	if source.PackageComponent == nil || strings.TrimSpace(source.Text) != "" {
		if err := resolveQuerySelectorViews(component); err != nil {
			return nil, err
		}
	}
	var declaredViews map[string]*spec.View
	if declarations != nil {
		declaredViews = declarations.viewsByParam
	}
	viewBindings, err := loader.normalizeIndependentViewParams(component, declaredViews)
	if err != nil {
		return nil, fmt.Errorf("normalize independent view contracts: %w", err)
	}
	applySourceDefaults(component, source)
	if source.ColumnRefiner != nil {
		templateInput, compileErr := (&discoveryInputCompiler{
			component: component, declarations: declarations.generation,
			viewBindings: gen.ViewBindings(viewBindings), resolver: typeResolver, source: source,
		}).compile()
		if compileErr != nil {
			return nil, compileErr
		}
		if err = source.ColumnRefiner.RefineRoot(ctx, component, source.Resources, templateInput); err != nil {
			return nil, err
		}
		templateInput, compileErr = (&discoveryInputCompiler{
			component: component, declarations: declarations.generation,
			viewBindings: gen.ViewBindings(viewBindings), resolver: typeResolver, source: source,
		}).compile()
		if compileErr != nil {
			return nil, compileErr
		}
		if err = source.ColumnRefiner.RefineViews(ctx, component, source.Resources, templateInput); err != nil {
			return nil, err
		}
	} else if err := column.New(nil).ValidateSourceProjections(component, source.Resources); err != nil {
		return nil, err
	}
	enrichDescription(ctx, component, source.Docs)
	goHandler, err := source.GoHandler.Clone()
	if err != nil {
		return nil, err
	}
	veltyHandler := source.VeltyHandler.Clone()
	veltyHandler, err = resolveAuthoredHandler(prepared, component, goHandler, veltyHandler)
	if err != nil {
		span := pointSpan(source.Text, 0)
		var programErr *veltyProgramError
		if errors.As(err, &programErr) && programErr.end > programErr.start {
			span = Span{
				Start: sourceMap.Position(programErr.start),
				End:   sourceMap.Position(programErr.end),
			}
		}
		diagnostic := &Diagnostic{
			Code: "DQL-HANDLER", Severity: SeverityError, Message: err.Error(), Path: source.Path,
			Span: span,
		}
		return nil, &CompileError{Cause: err, Diagnostics: []*Diagnostic{diagnostic}}
	}
	return &Result{
		Source:                source,
		Component:             component,
		PreparedSQL:           prepared.SQL,
		Statements:            prepared.Statements,
		SourceMap:             sourceMap,
		TypeContext:           compiledTypeContext,
		TypeResolver:          typeResolver,
		TypeAuthority:         typecatalog.TranscribeAuthority,
		Declarations:          declarations.generation,
		ViewBindings:          gen.ViewBindings(viewBindings),
		GoHandler:             goHandler,
		VeltyHandler:          veltyHandler,
		ContractTypeOverrides: contractTypeOverrides,
	}, nil
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
	result := &typecatalog.ResolutionContext{}
	if source != nil {
		result.PackageDir = source.BaseDir()
		result.PackagePath = strings.TrimSpace(source.Scope)
		if result.PackagePath != "" {
			result.PackageName = path.Base(result.PackagePath)
		}
	}
	if authored != nil {
		result.DefaultPackage = authored.DefaultPackage
		for _, item := range authored.Imports {
			result.Imports = append(result.Imports, typecatalog.PackageImport{Alias: item.Alias, Package: item.Package})
		}
	}
	return typecatalog.NormalizeContext(result)
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
