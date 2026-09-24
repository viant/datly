package transcribe

import (
	"context"
	"fmt"
	"github.com/viant/bindly/resource"
	"os"
	"path/filepath"

	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
	smodel "github.com/viant/x/syntetic/model"
	"strings"
)

// GeneratedPackage holds transcribed package artifacts for package bootstrap.
type GeneratedPackage struct {
	Result  *gen.Result
	Package *smodel.Package
	Types   *typecatalog.Catalog
}

// Transcribe compiles one authored source under normalized options, emits its
// package artifacts, and loads the generated package.
func (c *Compiler) Transcribe(ctx context.Context, request Request) (*GeneratedPackage, error) {
	if request.Generation.Enabled() {
		generator, generation, err := request.GeneratorRequest()
		if err != nil {
			return nil, err
		}
		return generator.Generate(ctx, generation)
	}
	if request.Component != nil {
		var types *typecatalog.Catalog
		if request.Source != nil {
			types = request.Source.Types
		}
		return (&PackageCompilation{Source: request.Source, Component: request.Component, InputType: request.InputType, OutputType: request.OutputType, Options: request.Options, Types: types}).Transcribe(ctx, request.Destination)
	}
	options, err := normalizeOptions(request.Options)
	if err != nil {
		return nil, err
	}
	compileSource := request.Source
	if request.Source != nil && request.Source.Types == nil {
		copy := *request.Source
		copy.Types = typecatalog.NewCatalog()
		compileSource = &copy
	}
	compiled, err := c.Compile(ctx, compileSource)
	if err != nil {
		return nil, err
	}
	input, packageDir, err := generationInput(request.Destination, "generated", compiled)
	if err != nil {
		return nil, err
	}
	if compiled.Component.Static != nil {
		if options.Handler.Target != HandlerNone || options.Contracts != ContractsAuto {
			return nil, fmt.Errorf("static content does not accept handler or contract generation options")
		}
		return c.generateInputAt(ctx, request.Destination, packageDir, compiled, input)
	}
	handlers := newHandlerGeneration(compiled, &input, options)
	handlers.directory = filepath.Join(request.Destination, packageDir)
	if err = handlers.prepare(); err != nil {
		return nil, handlers.diagnostic(err)
	}
	return c.generateInputAt(ctx, request.Destination, packageDir, compiled, input)
}

func (c *Compiler) generateCompiled(ctx context.Context, rootDir string, compiled *Result) (*GeneratedPackage, error) {
	return c.generateCompiledAt(ctx, rootDir, "generated", compiled)
}

func (c *Compiler) generateCompiledAt(ctx context.Context, rootDir, packageDir string, compiled *Result) (*GeneratedPackage, error) {
	return c.generateCompiledAtWithPolicy(ctx, rootDir, packageDir, compiled, gen.GenerationPolicyMerge)
}

func (c *Compiler) generateCompiledAtWithPolicy(ctx context.Context, rootDir, packageDir string, compiled *Result, policy gen.GenerationPolicy) (*GeneratedPackage, error) {
	input, packageDir, err := generationInput(rootDir, packageDir, compiled)
	if err != nil {
		return nil, err
	}
	input.GenerationPolicy = policy
	return c.generateInputAt(ctx, rootDir, packageDir, compiled, input)
}

func (c *Compiler) generateInputAt(ctx context.Context, rootDir, packageDir string, compiled *Result, input gen.Input) (*GeneratedPackage, error) {
	pkgDir := filepath.Join(rootDir, packageDir)
	result, err := gen.New(input).Generate(pkgDir)
	if err != nil {
		return nil, err
	}
	for _, shapePlan := range result.Plan.ShapePackages {
		authority, e := typecatalog.NewDestinationAuthority(rootDir)
		if e != nil {
			return nil, e
		}
		dest, e := authority.Package(shapePlan.Package, "")
		if e != nil {
			return nil, e
		}
		shapePackage, e := loaderast.LoadPackageFS(ctx, os.DirFS(rootDir), filepath.ToSlash(dest.Directory))
		if e != nil {
			return nil, e
		}
		if e = result.RegisterPackage(compiled.Source.Types, shapePackage, filepath.Join(rootDir, dest.Directory)); e != nil {
			return nil, e
		}
	}
	pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(rootDir), filepath.ToSlash(packageDir))
	if err != nil {
		return nil, err
	}
	if err := result.RegisterPackage(compiled.Source.Types, pkg, pkgDir); err != nil {
		return nil, err
	}
	return &GeneratedPackage{
		Result:  result,
		Package: pkg,
		Types:   compiled.Source.Types,
	}, nil
}

func generationInput(rootDir, packageDir string, compiled *Result) (gen.Input, string, error) {
	if compiled == nil || compiled.Source == nil || compiled.Component == nil {
		return gen.Input{}, "", fmt.Errorf("compiled transcribe result is required")
	}
	if compiled.Source.Types == nil {
		return gen.Input{}, "", fmt.Errorf("compiled transcribe result requires type catalog")
	}
	authority, err := typecatalog.NewDestinationAuthority(rootDir)
	if err != nil {
		return gen.Input{}, "", err
	}
	authored := ""
	if compiled.Component.TypeContext != nil {
		authored = compiled.Component.TypeContext.PackagePath
	}
	destination, err := authority.Package(authored, packageDir)
	if err != nil {
		return gen.Input{}, "", err
	}
	packageDir = destination.Directory
	targetPackage := destination.ImportPath
	component := compiled.Component.Clone()
	if component.TypeContext != nil && authored != "" && component.TypeContext.DefaultPackage == authored {
		component.TypeContext.DefaultPackage = targetPackage
	}
	if err = resolveComponentSources(component, compiled.Source.Resources); err != nil {
		return gen.Input{}, "", err
	}
	resources := compiled.Source.Resources
	if base := compiled.Source.BaseDir(); base != "" {
		if resources == nil {
			resources = resource.New()
		}
		if _, ok := resources.Lookup(""); !ok {
			resources, err = resources.WithDefault(os.DirFS(base))
			if err != nil {
				return gen.Input{}, "", err
			}
		}
	}
	input := gen.Input{Resources: resources,
		Component: component, Declarations: compiled.Declarations,
		SQLResources:  true,
		TargetPackage: targetPackage, ProjectRoot: rootDir, Contracts: compiled.Contracts, Views: compiled.Views, ViewBindings: compiled.ViewBindings,
		GeneratedTypes:  compiled.GeneratedTypes,
		GoHandler:       compiled.GoHandler,
		ExternalHandler: compiled.ExternalHandler.Clone(),
		VeltyHandler:    compiled.VeltyHandler,
	}
	if strings.TrimSpace(authored) != "" {
		input.PackageName = destination.Name
	}
	if compiled.TypeResolver != nil {
		input.TypeResolver = compiled.TypeResolver
	}
	return input, packageDir, nil
}
