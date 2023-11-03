package signature

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/xreflect"
	"gopkg.in/yaml.v3"
	"reflect"
	"strings"
)

type (
	Header struct {
		URL       string
		Resource  *Resource       `yaml:"Resource"`
		Contracts []*ContractPath `yaml:"Routes"`
	}

	Type struct {
		Name       string             `yaml:"Name"`
		Package    string             `yaml:"Package"`
		Parameters []*state.Parameter `yaml:"Parameters"`
	}

	Output struct {
		Type *Type `yaml:"Type"`
	}
	Input struct {
		Type *Type `yaml:"Type"`
	}

	ContractPath struct {
		Method string  `yaml:"Method"`
		URI    string  `yaml:"URI"`
		Input  *Input  `yaml:"Input"`
		Output *Output `yaml:"Output"`
	}

	Resource struct {
		Types           []*view.TypeDefinition `yaml:"Types"`
		InputParameters []*state.Parameter     `yaml:"Parameters"`
	}
)

func (h *Header) buildInputType(aContract *ContractPath, registry *xreflect.Types, signature *Signature) {
	anInput := aContract.Input
	if anInput == nil || anInput.Type == nil {
		return
	}
	inputType := anInput.Type
	signature.Input = &state.Type{}
	signature.Input.Parameters = inputType.Parameters
	if inputType.Name != "" {
		signature.Input.Schema = &state.Schema{}
		signature.Input.Schema.Name = inputType.Name
		signature.Input.Schema.SetPackage(inputType.Package)
	}

}

func (p *ContractPath) ensureInput(aMatch *entry) {
	if p.Input == nil {
		p.Input = &Input{}
	}
	if p.Input.Type == nil {
		p.Input.Type = &Type{}
	}
	if len(p.Input.Type.Parameters) == 0 {
		p.Input.Type.Parameters = aMatch.header.Resource.InputParameters
	}
}

func (h *Header) Signature(aContract *ContractPath, registry *xreflect.Types) (*Signature, error) {
	signature := &Signature{URI: aContract.URI, Method: aContract.Method}
	h.buildInputType(aContract, registry, signature)
	h.buildFilterType(aContract, registry, signature)
	if err := h.buildOutputType(aContract, signature, registry); err != nil {
		return nil, err
	}
	return signature, nil
}

func (h *Header) buildFilterType(contract *ContractPath, registry *xreflect.Types, signature *Signature) {
	output := state.Parameters(contract.Output.Type.Parameters)
	filter := output.LookupByLocation(state.KindOutput, "filter")
	if filter == nil {
		return
	}
	schema := filter.Schema
	if schema.Type() != nil {
		return
	}

	if typeName := schema.TypeName(); typeName != "" {
		if rType, _ := registry.Lookup(typeName); rType != nil {
			return
		}
	}
	if schema.Name == "" {
		schema.Name = "Filter"
	}
	dataParameter := output.LookupByLocation(state.KindOutput, keys.ViewData)
	inputParameters := state.Parameters(contract.Input.Type.Parameters)
	predicateType := inputParameters.PredicateStructType(nil)
	if predicateType.NumField() > 0 {
		pkg := ""
		if dataParameter != nil {
			pkg = dataParameter.Schema.Package
		}
		signature.Types = append(signature.Types, &view.TypeDefinition{Name: schema.Name, DataType: predicateType.String(), Package: pkg})
		signature.Filter = state.NewSchema(predicateType)
		registry.Register(schema.Name, xreflect.WithTypeDefinition(signature.Filter.Type().String()))
	}
}

func (h *Header) buildOutputType(aContract *ContractPath, signature *Signature, registry *xreflect.Types) error {

	if aContract.Output == nil || aContract.Output.Type == nil {
		return nil
	}
	if len(aContract.Output.Type.Parameters) == 0 && aContract.Output.Type.Name != "" {
		//TODO check definition to see if this is inline type or external type
		signature.Output = &state.Schema{Name: aContract.Output.Type.Name}
	}

	parameters := state.Parameters(aContract.Output.Type.Parameters)
	isAnonymous := len(parameters) == 1 && strings.Contains(parameters[0].Tag, "anonymous")

	outputParameter := parameters.LookupByLocation(state.KindOutput, keys.ViewData)
	if reflect.StructTag(outputParameter.Tag).Get(xreflect.TagTypeName) == "" {
		outputParameter.Tag += ` ` + xreflect.TagTypeName + `:"` + outputParameter.Schema.Name + `"`
	}
	//pkg := aContract.Output.Type.Package

	var viewType *view.TypeDefinition
	for _, candidate := range h.Resource.Types {
		_ = registry.Register(candidate.Name, xreflect.WithTypeDefinition(candidate.DataType), xreflect.WithPackage(candidate.Package))
		if candidate.Name == outputParameter.Schema.Name {
			viewType = candidate
			break
		}
	}
	if viewType == nil {
		return fmt.Errorf("failed to match data component output type for path: %v:%v", aContract.Method, aContract.URI)
	}
	contract.EnsureParameterTypes(parameters, nil, nil, nil)

	if !isAnonymous {
		rType, err := registry.Lookup(viewType.Name, xreflect.WithPackage(viewType.Package))
		if err != nil {
			return fmt.Errorf("failed to build component signature %s: %w", viewType.Name, err)
		}
		cardinality := outputParameter.Schema.Cardinality
		if cardinality == state.Many {
			rType = reflect.PtrTo(rType)
		}

		outputType, err := parameters.ReflectType(aContract.Output.Type.Package, registry.Lookup)
		if err != nil {
			return fmt.Errorf("failed to get output type: %w", err)
		}
		componentSchema := state.NewSchema(outputType)
		componentSchema.Name = strings.Replace(outputParameter.Schema.Name, "View", "", 1) + "Output"
		componentSchema.Cardinality = state.One
		componentSchema.DataType = outputType.String()
		signature.Output = componentSchema
	} else {
		signature.Output = outputParameter.Schema
	}
	signature.Anonymous = isAnonymous

	signature.Types = append(signature.Types, viewType)
	return nil
}

var fs = afs.New()

func NewHeader(ctx context.Context, URL string) (*Header, error) {
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	ret := &Header{}
	if err = yaml.Unmarshal(data, ret); err != nil {
		return nil, err
	}
	ret.URL = URL
	return ret, nil
}
