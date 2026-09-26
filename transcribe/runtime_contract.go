package transcribe

import (
	"context"
	"fmt"
	"go/token"
	"path"
	"reflect"
	"strings"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
)

// RuntimeContract is the dynamic-reader contract compiled from DQL without
// emitting or loading a persistent generated Go package.
type RuntimeContract struct {
	Result     *Result
	Component  *spec.Component
	InputType  reflect.Type
	OutputType reflect.Type
	Resources  *resource.Store
	Types      *typecatalog.Catalog
	Plan       *gen.Plan
}

// RuntimeContracts compiles authored DQL and materializes generated input and
// output reflect types for an ephemeral reader execution. rootDir must be a
// module root so #package destination authority is resolved consistently with
// ordinary transcription.
func (c *Compiler) RuntimeContracts(ctx context.Context, rootDir string, source *Source) (*RuntimeContract, error) {
	if c == nil {
		c = NewCompiler()
	}
	if source == nil {
		return nil, fmt.Errorf("runtime contract source is required")
	}
	copy := *source
	if copy.Types == nil {
		copy.Types = typecatalog.NewCatalog()
	}
	compiled, err := c.Compile(ctx, &copy)
	if err != nil {
		return nil, err
	}
	input, _, err := generationInput(rootDir, "generated", compiled)
	if err != nil {
		return nil, err
	}
	return runtimeContractFromInput(input, compiled)
}

// RuntimeContractsInModule materializes an ephemeral reader without requiring
// a source checkout or go.mod at runtime. The module path is host-owned and
// authored #package must remain inside it; this path never emits Go files.
func (c *Compiler) RuntimeContractsInModule(ctx context.Context, modulePath string, source *Source) (*RuntimeContract, error) {
	if c == nil {
		c = NewCompiler()
	}
	modulePath = strings.TrimSuffix(strings.TrimSpace(modulePath), "/")
	if modulePath == "" || source == nil {
		return nil, fmt.Errorf("runtime module path and source are required")
	}
	copy := *source
	if copy.Types == nil {
		copy.Types = typecatalog.NewCatalog()
	}
	compiled, err := c.Compile(ctx, &copy)
	if err != nil {
		return nil, err
	}
	authored := ""
	if compiled.Component.TypeContext != nil {
		authored = strings.TrimSpace(compiled.Component.TypeContext.PackagePath)
	}
	if !strings.HasPrefix(authored, modulePath+"/") {
		return nil, fmt.Errorf("runtime package %q is outside module %q", authored, modulePath)
	}
	name := path.Base(authored)
	if !token.IsIdentifier(name) || token.Lookup(name).IsKeyword() || name == "_" {
		return nil, fmt.Errorf("runtime package %q has an invalid Go package name", authored)
	}
	component := compiled.Component.Clone()
	if err = resolveComponentSources(component, compiled.Source.Resources); err != nil {
		return nil, err
	}
	input := gen.Input{Resources: compiled.Source.Resources, Component: component,
		Declarations: compiled.Declarations, SQLResources: true,
		TargetPackage: authored, PackageName: name, Contracts: compiled.Contracts,
		Views: compiled.Views, ViewBindings: compiled.ViewBindings,
		GeneratedTypes: compiled.GeneratedTypes, GoHandler: compiled.GoHandler,
		ExternalHandler: compiled.ExternalHandler.Clone(), VeltyHandler: compiled.VeltyHandler}
	if compiled.TypeResolver != nil {
		input.TypeResolver = compiled.TypeResolver
	}
	return runtimeContractFromInput(input, compiled)
}

func runtimeContractFromInput(input gen.Input, compiled *Result) (*RuntimeContract, error) {
	generator := gen.New(input)
	inputType, err := generator.RuntimeInputType()
	if err != nil {
		return nil, err
	}
	outputType, err := generator.RuntimeOutputType()
	if err != nil {
		return nil, err
	}
	plan, err := generator.Plan()
	if err != nil {
		return nil, err
	}
	return &RuntimeContract{Result: compiled, Component: input.Component.Clone(), InputType: inputType, OutputType: outputType, Resources: input.Resources, Types: compiled.Source.Types, Plan: plan}, nil
}
