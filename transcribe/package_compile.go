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
	options, err := normalizeOptions(c.Options)
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
	handlers := newHandlerGeneration(compiled, &input, options)
	handlers.directory = filepath.Join(rootDir, packageDir)
	if err = handlers.prepare(); err != nil {
		return nil, handlers.diagnostic(err)
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
	if err = catalog.RegisterAll(typecatalog.TypeOriginPackage, descriptors...); err != nil {
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
		if err = c.Types.RegisterAll(typecatalog.TypeOriginPackage, descriptors...); err != nil {
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
	// Go-only discovery has no DQL destination directive. Recover only an
	// existing generated owner's destination, never infer one from type imports.
	if strings.TrimSpace(a.source.Text) == "" && a.route.Dir != "" {
		destination, err := (gen.PackageOwnership{Directory: a.route.Dir}).Destination(component.Key)
		if err != nil {
			return nil, err
		}
		if destination != "" {
			if component.TypeContext == nil {
				component.TypeContext = &spec.TypeContext{}
			}
			component.TypeContext.PackagePath = destination
		}
	}
	if _, err := (&componentLoader{}).normalizeIndependentViewParams(component, nil); err != nil {
		return nil, fmt.Errorf("normalize package view contracts: %w", err)
	}
	compileSource := *a.source
	compileSource.PackageComponent = component
	compileSource.Types = a.catalog
	result, err := NewCompiler().Compile(ctx, &compileSource)
	if err != nil {
		return nil, err
	}
	contracts := &contractLinker{
		packageComponent: component, compiledComponent: result.Component, source: a.route,
		input: a.input, output: a.output,
	}
	result.Contracts = contracts.references()
	if result.Contracts.Output != nil && a.root != nil {
		result.Views = gen.ViewReferences{
			gen.RootViewPath: {DescriptorKey: a.root.Key()},
		}
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
		result.Views[linked.identity] = &gen.ViewReference{DescriptorKey: linked.descriptor.Key()}
	}
	contracts.prepareGeneratedTypes(result.Component, result.Contracts, result.ContractTypeOverrides)
	result.TypeAuthority = typecatalog.PackageAuthority
	result.TypeResolver, err = typecatalog.NewResolver(a.catalog, result.TypeAuthority, result.TypeContext)
	if err != nil {
		return nil, err
	}
	return result, nil
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
	component, err := c.packageSource.ResolveDescriptors(resolver)
	if err != nil {
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
