package bootstrap

import (
	"context"
	"github.com/viant/datly/constant"
	documentation "github.com/viant/datly/documentation"
	xdocs "github.com/viant/xdatly/docs"
	"reflect"

	"github.com/viant/bindly/resource"
	rhandler "github.com/viant/datly/runtime/handler"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/output"
	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	runtimeRegistry "github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	sqlreader "github.com/viant/datly/sql/reader"
	readercompiler "github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	xcodec "github.com/viant/xdatly/codec"
)

// ArtifactInput is resolved bootstrap input. Authored DQL parsing belongs to
// transcribe; this stage only compiles runtime plans from spec and Go types.
type ArtifactInput struct {
	Const         *constant.Values
	Documentation xdocs.Source
	Component     *spec.Component
	InputType     reflect.Type
	OutputType    reflect.Type
	Handler       rhandler.TypedHandler
	CodecFactory  xcodec.Factory
	Types         *typecatalog.Catalog
	// HandlerOwnedOutput preserves the output type/schema without importing its
	// reader declarations into a component whose handler produces that output.
	HandlerOwnedOutput bool
	DirectViewField    string
	// Resources is the same Bindly store supplied to runtime composition. SQL,
	// reader, and codec compilers consume it only through the fs.FS contract.
	Resources *resource.Store
}

// Artifact is bootstrap's compiled output before runtime registration binds a
// concrete SQL reader execution to its connections and caches.
type Artifact struct {
	instanceConst    *constant.Values
	Documentation    *documentation.Snapshot
	Handler          rhandler.TypedHandler
	Component        *spec.Component
	Input            *runtimeRegistry.InputContract
	Output           *runtimeRegistry.OutputContract
	Reader           *sqlreader.Plan
	ViewDependencies []*sqlreader.ViewDependency
	inputType        reflect.Type
	outputType       reflect.Type
}

// BuildArtifact composes the handler-input and reader-plan compilers. Registry
// receives the completed artifact and performs no compilation.
func BuildArtifact(input ArtifactInput) (*Artifact, error) {
	builder, err := NewArtifactBuilder(nil)
	if err != nil {
		return nil, err
	}
	return builder.Build(input)
}

type artifactCompiler struct {
	input ArtifactInput
}

func (c *artifactCompiler) compile() (*Artifact, error) {
	input := c.input
	if input.Const != nil {
		if _, err := input.Const.For(input.Component); err != nil {
			return nil, err
		}
		input.Component = input.Const.Apply(input.Component)
	}
	outputDescriptor := linkedContractType(input.OutputType)
	if input.HandlerOwnedOutput {
		outputDescriptor = nil
	}
	component, err := (ContractResolver{
		Component: input.Component, InputType: linkedContractType(input.InputType), OutputType: outputDescriptor,
	}).Resolve()
	if err != nil {
		return nil, err
	}
	// Reader components resolve selectors against the completed view index in
	// the reader compiler below. A handler-owned output never builds that index
	// and links no output views, so its spec graph is final: reject unknown or
	// ambiguous selector targets here, before the component can be registered.
	if input.HandlerOwnedOutput {
		if err = ResolveQuerySelectorViews(component, true); err != nil {
			return nil, err
		}
	}
	if err = input.Const.Validate(component, c.lookupType); err != nil {
		return nil, err
	}
	if err = c.compileOutputColumns(component); err != nil {
		return nil, err
	}
	typeContext := &typecatalog.ResolutionContext{PackagePath: component.Key.Scope}
	if authored := component.TypeContext; authored != nil {
		typeContext.DefaultPackage = authored.DefaultPackage
		for _, item := range authored.Imports {
			typeContext.Imports = append(typeContext.Imports, typecatalog.PackageImport{Alias: item.Alias, Package: item.Package})
		}
	}
	if err = (readerpredicate.DefinitionCompiler{Context: typeContext}).Compile(component); err != nil {
		return nil, err
	}
	input.Component = component
	effective, err := input.Const.For(component)
	if err != nil {
		return nil, err
	}
	factory := newCodecFactory(input.CodecFactory)
	compiledInput, err := handlercompiler.New(handlercompiler.Input{
		Component: input.Component, InputType: input.InputType,
		CodecFactory: factory, Resources: effective.Resources(input.Resources), TypeLookup: c.lookupType,
	}).Compile()
	if err != nil {
		return nil, err
	}
	predicate, err := readerpredicate.Compile(readerpredicate.CompileInput{
		Component: input.Component, InputType: input.InputType, Bindings: compiledInput.Bindings, Lookup: c.lookupType,
	})
	if err != nil {
		return nil, err
	}
	var readerPlan *sqlreader.Plan
	var viewDependencies []*sqlreader.ViewDependency
	if input.Component.RootView != nil || len(input.Component.Views) > 0 {
		if !input.HandlerOwnedOutput {
			readerPlan, err = readercompiler.Compile(readercompiler.Input{
				CodecFactory: factory,
				Component:    input.Component, InputType: input.InputType, OutputType: input.OutputType,
				Bindings:  compiledInput.Bindings,
				Predicate: predicate, TypeLookup: c.lookupType,
				Const: input.Const, DirectViewField: input.DirectViewField, Resources: input.Resources,
			})
			if err != nil {
				return nil, err
			}
		}
		viewDependencies, err = readercompiler.CompileViewDependencies(readercompiler.Input{
			CodecFactory: factory,
			Component:    input.Component, InputType: input.InputType, OutputType: input.OutputType,
			Bindings:  compiledInput.Bindings,
			Predicate: predicate, TypeLookup: c.lookupType,
			Const: input.Const, Resources: input.Resources,
		})
		if err != nil {
			return nil, err
		}
	}
	dataField := ""
	if readerPlan != nil {
		dataField = readerPlan.OutputViewField
	}
	outputContract, err := (output.Compiler{Lookup: c.lookupType}).Compile(output.CompileInput{Component: component, Type: input.OutputType, DataField: dataField})
	if err != nil {
		return nil, err
	}
	docs, err := (documentation.Loader{Resources: input.Resources}).Load(context.Background(), input.Documentation, component.Documentation)
	if err != nil {
		return nil, err
	}
	docs, err = docs.ForComponent(component, input.OutputType)
	if err != nil {
		return nil, err
	}
	return &Artifact{instanceConst: input.Const, Documentation: docs,
		Component: component, Input: compiledInput.Input.WithDocumentation(docs),
		Output: outputContract,
		Reader: readerPlan, ViewDependencies: viewDependencies,
		Handler: input.Handler, inputType: input.InputType, outputType: input.OutputType,
	}, nil
}

func linkedContractType(typeOf reflect.Type) *x.Type {
	shape := xshape.Linked(typeOf)
	if shape == nil {
		return nil
	}
	return shape.Descriptor()
}

func (c *artifactCompiler) lookupType(name string) (reflect.Type, error) {
	if c == nil || c.input.Types == nil {
		return nil, nil
	}
	resolution := &typecatalog.ResolutionContext{}
	if component := c.input.Component; component != nil {
		resolution.PackagePath = component.Key.Scope
		if authored := component.TypeContext; authored != nil {
			resolution.DefaultPackage = authored.DefaultPackage
			for _, imported := range authored.Imports {
				resolution.Imports = append(resolution.Imports, typecatalog.PackageImport{Alias: imported.Alias, Package: imported.Package})
			}
		}
	}
	resolver, err := typecatalog.NewResolver(c.input.Types, typecatalog.PackageAuthority, resolution)
	if err != nil {
		return nil, err
	}
	return resolver.Type(name)
}
