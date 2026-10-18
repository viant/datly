package transcribe

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/handlergen"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

// PackageCompilation joins an explicitly selected package component with an
// authored DQL source. Package tags establish the base contract; DQL overlays
// that contract through Compiler's canonical load stage.
type PackageCompilation struct {
	Source     *Source
	Component  *bootstrap.RouteSource
	InputType  reflect.Type
	OutputType reflect.Type
	Types      *typecatalog.Catalog
	Options    Options
}

type packageAuthority struct {
	source     *Source
	route      *bootstrap.RouteSource
	component  *spec.Component
	catalog    *typecatalog.Catalog
	input      *x.Type
	output     *x.Type
	root       *x.Type
	inputViews []linkedInputView
}

type descriptorPackageCompilation struct {
	source        *Source
	packageSource *bootstrap.PackageComponentSource
	catalog       *typecatalog.Catalog
}

// Transcribe compiles package/DQL authority under normalized options and emits
// only products selected by the resulting canonical ownership decisions.
func (c *PackageCompilation) Transcribe(ctx context.Context, rootDir string) (*GeneratedPackage, error) {
	if c == nil {
		return nil, fmt.Errorf("package compilation is required")
	}
	options, err := handlergen.Normalize(c.Options)
	if err != nil {
		return nil, err
	}
	compiled, err := c.Compile(ctx)
	if err != nil {
		return nil, err
	}
	input, packageDir, err := generationInput(rootDir, "generated", compiled)
	if err != nil {
		return nil, err
	}
	handlers := handlergen.New(compiled.handlerContext(), &input, options)
	handlers.Directory = filepath.Join(input.ProjectRoot, packageDir)
	if err = handlers.Prepare(); err != nil {
		return nil, compiled.handlerDiagnostic(err)
	}
	return NewCompiler().generateInputAt(ctx, rootDir, packageDir, compiled, input)
}

// Compile registers linked contract types under package authority and compiles
// the DQL source with package metadata as its canonical base.
func (c *PackageCompilation) Compile(ctx context.Context) (*Result, error) {
	if c == nil {
		return nil, fmt.Errorf("package compilation is required")
	}
	if c.Source == nil {
		return nil, ErrNilSource
	}
	if c.Component == nil {
		return nil, fmt.Errorf("package component is required")
	}
	if scope, packagePath := strings.TrimSpace(c.Source.Scope), strings.TrimSpace(c.Component.PackagePath); scope == "" || packagePath == "" || scope != packagePath {
		return nil, fmt.Errorf("DQL scope %q does not match component package %q", scope, packagePath)
	}
	if c.InputType == nil || c.OutputType == nil {
		return nil, fmt.Errorf("linked input and output types are required for package transcription")
	}
	if err := c.Component.ValidateContractTypes(c.InputType, c.OutputType); err != nil {
		return nil, err
	}

	component, err := c.Component.Resolve(c.InputType, c.OutputType)
	if err != nil {
		return nil, err
	}
	baseCatalog := c.Types
	if baseCatalog == nil {
		baseCatalog = typecatalog.NewCatalog()
	}
	catalog, err := baseCatalog.Clone()
	if err != nil {
		return nil, err
	}
	views := newPackageViewResolver(component, nil)
	inputDescriptor, err := views.linkedDescriptor(c.InputType)
	if err != nil {
		return nil, fmt.Errorf("register package input: %w", err)
	}
	outputDescriptor, err := views.linkedDescriptor(c.OutputType)
	if err != nil {
		return nil, fmt.Errorf("register package output: %w", err)
	}
	viewDescriptor, err := views.rootDescriptor(c.OutputType)
	if err != nil {
		return nil, fmt.Errorf("register package root view: %w", err)
	}
	inputViews, err := views.inputDescriptors(c.InputType)
	if err != nil {
		return nil, fmt.Errorf("register package input views: %w", err)
	}
	descriptors := []*x.Type{inputDescriptor, outputDescriptor}
	if viewDescriptor != nil {
		descriptors = append(descriptors, viewDescriptor)
	}
	for _, linked := range inputViews {
		descriptors = append(descriptors, linked.descriptor)
	}
	if err = catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, descriptors...); err != nil {
		return nil, err
	}
	result, err := (&packageAuthority{
		source: c.Source, route: c.Component, component: component, catalog: catalog,
		input: inputDescriptor, output: outputDescriptor, root: viewDescriptor, inputViews: inputViews,
	}).compile(ctx)
	if err != nil {
		return nil, err
	}
	if c.Types != nil {
		if err = c.Types.LinkRuntimeAll(typecatalog.TypeOriginPackage, descriptors...); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (a *packageAuthority) compile(ctx context.Context) (*Result, error) {
	if a == nil || a.source == nil || a.route == nil || a.component == nil || a.catalog == nil {
		return nil, fmt.Errorf("complete package compilation authority is required")
	}
	component := a.component.Clone()
	// A Go holder declares the component scope; imported contract types never
	// choose the component destination.
	if strings.TrimSpace(a.source.Text) == "" && a.route.Dir != "" && component.Key.Scope != "" {
		if component.TypeContext == nil {
			component.TypeContext = &spec.TypeContext{}
		}
		component.TypeContext.PackagePath = component.Key.Scope
	}
	if _, err := link.NormalizeIndependentViewParams(component, nil); err != nil {
		return nil, fmt.Errorf("normalize package view contracts: %w", err)
	}
	compileSource := *a.source
	compileSource.PackageComponent = component
	compileSource.Types = a.catalog
	compileSource.LinkedInputType = a.input.Type
	compileSource.LinkedOutputType = a.output.Type
	compileSource.LinkedHandler = a.route.LinkedHandler
	if strings.TrimSpace(a.source.Text) == "" {
		resources, err := packageInputDefaultResources(compileSource.Resources, compileSource.LinkedInputType, a.route.PackagePath)
		if err != nil {
			return nil, err
		}
		compileSource.Resources = resources
	}
	// The holder was admitted before this detached explicit DQL authoring pass.
	// Its previous immutable graph cannot authorize the newly compiled graph.
	if strings.TrimSpace(a.source.Text) != "" {
		component.ReaderGraph = nil
	}
	result, err := NewCompiler().Compile(ctx, &compileSource)
	if err != nil {
		return nil, err
	}
	if a.component.ReaderGraph != nil {
		result.Source.PackageComponent = a.component.Clone()
		result.Source.ReaderMetadataURI = a.route.Tag.ReaderMetadata
	}
	contracts := &link.Contracts{
		PackageComponent: component, CompiledComponent: result.Component, Source: a.route,
		Input: a.input, Output: a.output,
	}
	result.Contracts = contracts.References()
	if strings.TrimSpace(a.source.Text) == "" {
		result.Contracts.Input = link.Reference(a.route.InputType, a.input)
		result.Contracts.Output = link.Reference(a.route.OutputType, a.output)
	}
	// Explicit qualified DQL output authority composes with the strict implicit
	// role decision. Validate through the same resolver generation consumes.
	result.TypeAuthority = typecatalog.PackageAuthority
	result.TypeResolver, err = typecatalog.NewResolver(a.catalog, result.TypeAuthority, result.TypeContext)
	if err != nil {
		return nil, err
	}
	var currentOutput *x.Type
	if strings.TrimSpace(a.source.Text) != "" {
		currentOutput = a.output
	}
	if err = linkExplicitOutputContract(result, result.ContractTypeOverrides.Output, currentOutput); err != nil {
		return nil, err
	}
	if err = retainPackageOutputRoot(result, a.output, a.root); err != nil {
		return nil, err
	}
	compiledViews := newPackageViewResolver(result.Component, nil)
	for _, linked := range a.inputViews {
		compiledView := compiledViews.byIdentity(linked.identity)
		if compiledView == nil || !compiledViews.equal(linked.view, compiledView) {
			continue
		}
		if result.Views == nil {
			result.Views = gen.ViewReferences{}
		}
		if prior := result.Views[linked.identity]; prior != nil && prior.Borrowed != nil {
			continue
		}
		result.Views[linked.identity] = &gen.ViewReference{DescriptorKey: linked.descriptor.Key()}
	}
	contracts.PrepareGeneratedTypes(result.Component, result.Contracts, result.ContractTypeOverrides.Input, result.ContractTypeOverrides.Output)
	return result, nil
}

// A package's row belongs only to that package output. Keep independently
// validated row authority and its receipts; an explicit output cannot replace it.
func retainPackageOutputRoot(result *Result, output, root *x.Type) error {
	if result == nil || result.Contracts.Output == nil || output == nil || root == nil || result.Contracts.Output.DescriptorKey != output.Key() {
		return nil
	}
	if result.Views == nil {
		result.Views = gen.ViewReferences{}
	}
	if prior := result.Views[gen.RootViewPath]; prior != nil {
		if prior.DescriptorKey != root.Key() {
			return fmt.Errorf("output contract has conflicting root row type authorities")
		}
		return nil
	}
	result.Views[gen.RootViewPath] = &gen.ViewReference{DescriptorKey: root.Key()}
	return nil
}

func (c *descriptorPackageCompilation) compile(ctx context.Context) (*Result, error) {
	if c == nil || c.source == nil || c.packageSource == nil || len(c.packageSource.Routes) == 0 || c.catalog == nil {
		return nil, fmt.Errorf("complete descriptor package authority is required")
	}
	context := &typecatalog.ResolutionContext{
		DefaultPackage: c.packageSource.Routes[0].PackagePath,
		PackageName:    c.packageSource.Routes[0].PackageName,
		PackagePath:    c.packageSource.Routes[0].PackagePath,
		PackageDir:     c.packageSource.Routes[0].Dir,
	}
	for _, item := range c.packageSource.Routes[0].Imports {
		context.Imports = append(context.Imports, typecatalog.PackageImport{Alias: item.Alias, Package: item.Package})
	}
	resolver, err := typecatalog.NewResolver(c.catalog, typecatalog.PackageAuthority, context)
	if err != nil {
		return nil, err
	}
	var component *spec.Component
	if strings.TrimSpace(c.source.Text) != "" {
		component, err = resolvePackageDeclarations(c.packageSource, resolver)
	} else {
		component, err = c.packageSource.ResolveDescriptors(resolver)
	}
	if err != nil {
		return nil, err
	}
	c.source.LinkedInputType = c.packageSource.InputType.Type
	c.source.LinkedOutputType = c.packageSource.OutputType.Type
	c.source.LinkedHandler = c.packageSource.Handler
	// Concrete linked contracts are the runtime authority for reachable view
	// shapes. Source AST remains useful for metadata, but it cannot manufacture
	// reflect.Type values for imported structs in another module.
	linkedViews := newPackageViewResolver(component, nil)
	linkedRoot, err := linkedViews.rootDescriptor(c.source.LinkedOutputType)
	if err != nil {
		return nil, err
	}
	linkedInputs, err := linkedViews.inputDescriptors(c.source.LinkedInputType)
	if err != nil {
		return nil, err
	}
	linkedDescriptors := []*x.Type{c.packageSource.InputType, c.packageSource.OutputType}
	if linkedRoot != nil {
		linkedDescriptors = append(linkedDescriptors, linkedRoot)
	}
	for _, linked := range linkedInputs {
		linkedDescriptors = append(linkedDescriptors, linked.descriptor)
	}
	// Runtime identity enriches the selected source declaration without changing
	// its generated ownership. LinkRuntimeAll only visits these exact descriptors.
	var generatedRuntime, packageRuntime []*x.Type
	for _, descriptor := range linkedDescriptors {
		if descriptor == nil {
			return nil, fmt.Errorf("type is required")
		}
		origin, _, lookupErr := c.catalog.Origin(typecatalog.PackageAuthority, descriptor.Key())
		if lookupErr != nil {
			return nil, lookupErr
		}
		if origin == typecatalog.TypeOriginGenerated {
			generatedRuntime = append(generatedRuntime, descriptor)
		} else {
			packageRuntime = append(packageRuntime, descriptor)
		}
	}
	if err = c.catalog.LinkRuntimeAll(typecatalog.TypeOriginGenerated, generatedRuntime...); err != nil {
		return nil, err
	}
	if err = c.catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, packageRuntime...); err != nil {
		return nil, err
	}
	resolvedContext := compileTypeContext(c.source, component.TypeContext)
	resolvedContext.PackageDir = c.packageSource.Routes[0].Dir
	resolvedContext.PackageName = c.packageSource.Routes[0].PackageName
	resolvedContext.PackagePath = c.packageSource.Routes[0].PackagePath
	resolver, err = typecatalog.NewResolver(c.catalog, typecatalog.PackageAuthority, resolvedContext)
	if err != nil {
		return nil, err
	}
	views := newPackageViewResolver(component, resolver)
	rootDescriptor, err := views.rootDescriptorFromMetadata()
	if err != nil {
		return nil, err
	}
	inputViews, err := views.inputDescriptorsFromMetadata()
	if err != nil {
		return nil, err
	}
	return (&packageAuthority{
		source: c.source, route: c.packageSource.Routes[0], component: component, catalog: c.catalog,
		input: c.packageSource.InputType, output: c.packageSource.OutputType, root: rootDescriptor, inputViews: inputViews,
	}).compile(ctx)
}
