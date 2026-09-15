package generate

import (
	"fmt"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

// Declaration contains parser-derived facts required by code generation.
// Transcribe owns parsing; generation consumes only this normalized product.
type Declaration struct {
	Projection       []DeclarationProjection
	NestedChildField string
	DataType         string
}

// DeclarationProjection is one typed StructQL result field produced by the
// transcribe parser. Generation never reparses declaration SQL.
type DeclarationProjection struct {
	Name      string
	Source    string
	Aggregate bool
}

type Declarations map[string]Declaration

type Input struct {
	Resources       *resource.Store
	Component       *spec.Component
	Declarations    Declarations
	TypeResolver    *typecatalog.Resolver
	TargetPackage   string
	PackageName     string
	ProjectRoot     string
	Contracts       ContractReferences
	Views           ViewReferences
	ViewBindings    ViewBindings
	GeneratedTypes  []GeneratedTypeReference
	SetMarkerViews  map[string]bool
	GoHandler       *GoHandlerAsset
	ContractHandler *ContractHandlerAsset
	MutationHandler *MutationHandlerAsset
	HookScaffold    *HookScaffoldAsset
	VeltyHandler    *VeltyHandlerAsset
	SQLResources    bool
	EntitySupport   *EntitySupportAsset
}

type ContractReference struct {
	Expression    string
	DescriptorKey string
}

type ContractReferences struct {
	Input  *ContractReference
	Output *ContractReference
}

const RootViewPath = "root"

// ViewReference identifies a package-owned canonical view type.
type ViewReference struct {
	DescriptorKey string
}

type ViewReferences map[string]*ViewReference

// ViewBindings maps one effective parameter identity to the exact canonical
// independent-view identity selected during transcribe/load assembly.
type ViewBindings map[string]string

type Generator struct {
	input    Input
	resolver typeResolver
	initErr  error
}

type typeResolver interface {
	Descriptor(string) (*x.Type, error)
}

func New(input Input) *Generator {
	input.Component = input.Component.Clone()
	input.Declarations = cloneDeclarations(input.Declarations)
	input.Views = cloneViewReferences(input.Views)
	input.ViewBindings = cloneViewBindings(input.ViewBindings)
	input.GeneratedTypes = append([]GeneratedTypeReference(nil), input.GeneratedTypes...)
	input.SetMarkerViews = cloneIdentitySet(input.SetMarkerViews)
	var cloneErr error
	input.GoHandler, cloneErr = input.GoHandler.Clone()
	var contractErr error
	input.ContractHandler, contractErr = input.ContractHandler.Clone()
	if cloneErr == nil {
		cloneErr = contractErr
	}
	var mutationErr error
	input.MutationHandler, mutationErr = input.MutationHandler.Clone()
	if cloneErr == nil {
		cloneErr = mutationErr
	}
	var hookErr error
	input.HookScaffold, hookErr = input.HookScaffold.Clone()
	if cloneErr == nil {
		cloneErr = hookErr
	}
	input.VeltyHandler = input.VeltyHandler.Clone()
	var entityErr error
	input.EntitySupport, entityErr = input.EntitySupport.Clone()
	if cloneErr == nil {
		cloneErr = entityErr
	}
	var resolver typeResolver
	if input.TypeResolver != nil {
		resolver = input.TypeResolver
	}
	return &Generator{input: input, resolver: resolver, initErr: cloneErr}
}

func (g *Generator) Plan() (*Plan, error) {
	return g.plan(true)
}

func (g *Generator) plan(requireConcreteHelpers bool) (*Plan, error) {
	if g == nil {
		return nil, fmt.Errorf("generator is required")
	}
	if g.initErr != nil {
		return nil, g.initErr
	}
	if g.input.Component == nil {
		return nil, fmt.Errorf("generation component is required")
	}
	return (&planResolver{input: g.input, types: g.resolver, requireConcreteHelpers: requireConcreteHelpers}).resolve()
}

func cloneViewReferences(source ViewReferences) ViewReferences {
	if source == nil {
		return nil
	}
	result := make(ViewReferences, len(source))
	for path, reference := range source {
		if reference == nil {
			result[path] = nil
			continue
		}
		copy := *reference
		result[path] = &copy
	}
	return result
}

func cloneViewBindings(source ViewBindings) ViewBindings {
	if len(source) == 0 {
		return nil
	}
	result := make(ViewBindings, len(source))
	for paramIdentity, viewIdentity := range source {
		result[paramIdentity] = viewIdentity
	}
	return result
}

func cloneIdentitySet(source map[string]bool) map[string]bool {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]bool, len(source))
	for identity, enabled := range source {
		result[identity] = enabled
	}
	return result
}

func (g *Generator) Generate(dir string) (*Result, error) {
	plan, err := g.Plan()
	if err != nil {
		return nil, err
	}
	files, err := EmitScaffold(dir, plan)
	if err != nil {
		return nil, err
	}
	return &Result{Plan: plan, Files: files}, nil
}

func (d Declarations) declaration(param *spec.Parameter) Declaration {
	if param == nil {
		return Declaration{}
	}
	return d[param.Identity()]
}

func cloneDeclarations(source Declarations) Declarations {
	if len(source) == 0 {
		return nil
	}
	result := make(Declarations, len(source))
	for name, declaration := range source {
		declaration.Projection = append([]DeclarationProjection(nil), declaration.Projection...)
		result[name] = declaration
	}
	return result
}
