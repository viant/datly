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
	predicateType := contract.Input.PredicateStructType(nil)
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
	if aContract.Output == nil || aContract.Output.Type == nil || len(aContract.Output.Type.Parameters) == 0 {
		return nil
	}
	parameters := state.Parameters(aContract.Output.Type.Parameters)
	isAnonymous := len(parameters) == 1 && strings.Contains(parameters[0].Tag, "anonymous")

	outputParameter := parameters.LookupByLocation(state.KindOutput, keys.ViewData)
	if reflect.StructTag(outputParameter.Tag).Get(xreflect.TagTypeName) == "" {
		outputParameter.Tag += ` ` + xreflect.TagTypeName + `:"` + outputParameter.Schema.Name + `"`
	}

	var viewType *view.TypeDefinition
	for _, candidate := range h.Resource.Types {
		_ = registry.Register(candidate.Name, xreflect.WithTypeDefinition(candidate.DataType))
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
		rType, _ := registry.Lookup(viewType.Name)
		cardinality := outputParameter.Schema.Cardinality
		if cardinality == state.Many {
			rType = reflect.PtrTo(rType)
		}

		outputType, err := parameters.ReflectType("github.com/viant/datly/view/autogen", registry.Lookup, false)
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
