package contract

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/repository/component"
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
		Contracts []*ContractPath `yaml:"Items"`
	}

	Type struct {
		Parameters []*state.Parameter `yaml:"Parameters"`
	}

	Output struct {
		Type *Type `yaml:"Type"`
	}

	ContractPath struct {
		Method string  `yaml:"Method"`
		URI    string  `yaml:"URI"`
		Output *Output `yaml:"Output"`
		Input  state.Parameters
	}

	Resource struct {
		Types           []*view.TypeDefinition `yaml:"Types"`
		InputParameters []*state.Parameter     `yaml:"Parameters"`
	}
)

func (h *Header) Signature(contract *ContractPath, registry *xreflect.Types) (*Signature, error) {

	signature := &Signature{URI: contract.URI, Method: contract.Method}

	h.buildFilterType(contract, registry, signature)
	if err := h.buildOutputType(contract, signature, registry); err != nil {
		return nil, err
	}
	return signature, nil
}

func (h *Header) buildFilterType(contract *ContractPath, registry *xreflect.Types, signature *Signature) {
	predicateType := contract.Input.PredicateStructType()
	if predicateType.NumField() > 0 {
		signature.Filter = state.NewSchema(predicateType)
		registry.Register("Filter", xreflect.WithTypeDefinition(signature.Filter.Type().String()))
	}
}

func (h *Header) buildOutputType(contract *ContractPath, signature *Signature, registry *xreflect.Types) error {
	if contract.Output == nil || contract.Output.Type == nil || len(contract.Output.Type.Parameters) == 0 {
		return nil
	}
	parameters := state.Parameters(contract.Output.Type.Parameters)
	isAnonymous := len(parameters) == 1 && strings.Contains(parameters[0].Tag, "anonymous")

	outputParameter := parameters.LookupByLocation(state.KindOutput, "data")

	var viewType *view.TypeDefinition
	for _, candidate := range h.Resource.Types {
		_ = registry.Register(candidate.Name, xreflect.WithTypeDefinition(candidate.DataType))
		if candidate.Name == outputParameter.Schema.Name {
			viewType = candidate
			break
		}
	}
	if viewType == nil {
		return fmt.Errorf("failed to match data component output type for path: %v:%v", contract.Method, contract.URI)
	}
	component.EnsureOutputKindParameterTypes(parameters, nil)
	if !isAnonymous {
		rType, _ := registry.Lookup(viewType.Name)
		cardinality := outputParameter.Schema.Cardinality
		if cardinality == state.Many {
			rType = reflect.PtrTo(rType)
		}
		outputParameter.Schema.SetType(rType)
		outputParameter.Schema.Cardinality = state.One
		outputType, err := parameters.ReflectType("github.com/viant/datly/view/autogen", registry.Lookup, false)
		if err != nil {
			return fmt.Errorf("failed to get output type: %w", err)
		}
		typeName := outputParameter.Schema.Name
		outputParameter.Schema = state.NewSchema(outputType)
		outputParameter.Schema.Name = typeName
		outputParameter.Schema.Cardinality = state.One
		viewType.DataType = outputType.String()
	}
	signature.Output = outputParameter.Schema
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
