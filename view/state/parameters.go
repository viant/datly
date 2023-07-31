package state

import (
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/xunsafe"
	"reflect"
)

type (
	ParameterOption func(p *Parameter)

	//Location tells how to get parameter value.

)

func (p *Parameter) Value(values interface{}) (interface{}, error) {
	ptr := xunsafe.AsPointer(values)
	return p.selector.Value(ptr), nil
}

func (p *Parameter) OutputType() reflect.Type {
	if p.Output != nil && p.Output.Schema != nil {
		return p.Output.Schema.Type()
	}

	return p.Schema.Type()
}

// NamedParameters represents Parameter map indexed by Parameter.Name
type NamedParameters map[string]*Parameter

// Parameters represents slice of parameters
type Parameters []*Parameter

func (p Parameters) FilterByKind(kind Kind) Parameters {
	var result = Parameters{}
	for i, candidate := range p {
		if candidate.In.Kind == kind {
			result = append(result, p[i])
		}
	}
	return result
}

func (p Parameters) Len() int {
	return len(p)
}

func (p Parameters) Less(i, j int) bool {
	if p[j].ErrorStatusCode == 401 {
		return false
	}

	if p[j].ErrorStatusCode == 403 {
		return p[i].ErrorStatusCode == 401
	}

	return true
}

func (p Parameters) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// Append appends parameter
func (p *Parameters) Append(parameter *Parameter) {
	for _, param := range *p {
		if param.Name == parameter.Name {
			return
		}
	}
	*p = append(*p, parameter)
}

// Lookup returns match parameter or nil
func (p Parameters) Lookup(name string) *Parameter {
	for _, param := range p {
		if param.Name == name {
			return param
		}
	}
	return nil
}

// Checks if parameters are valid
func (p Parameters) Validate() error {
	result := NamedParameters(make(map[string]*Parameter))
	for i, parameter := range p {
		if _, ok := result[parameter.Name]; ok {
			return fmt.Errorf("parameter with %v name already exists in given resource", parameter.Name)
		}
		if err := p[i].Validate(); err != nil {
			return err
		}
		result[parameter.Name] = p[i]
	}
	return nil
}

// Index indexes parameters by Parameter.Name
func (p Parameters) Index() NamedParameters {
	result := NamedParameters(make(map[string]*Parameter))
	for i, parameter := range p {
		result[parameter.Name] = p[i]
	}
	return result
}

// Filter filters Parameters with given Kind and creates Template
func (p Parameters) Filter(kind Kind) NamedParameters {
	result := make(map[string]*Parameter)

	for parameterIndex := range p {
		if p[parameterIndex].In.Kind != kind {
			continue
		}
		result[p[parameterIndex].In.Name] = p[parameterIndex]

	}

	return result
}

func (p NamedParameters) Merge(with NamedParameters) {
	for s := range with {
		p[s] = with[s]
	}
}

// Lookup returns Parameter with given name
func (p NamedParameters) Lookup(paramName string) (*Parameter, error) {
	if param, ok := p[paramName]; ok {
		return param, nil
	}
	return nil, fmt.Errorf("not found parameter %v", paramName)
}

// Register registers parameter
func (p NamedParameters) Register(parameter *Parameter) error {
	if _, ok := p[parameter.Name]; ok {
		fmt.Printf("[WARN] parameter with %v name already exists in given resource", parameter.Name)
	}
	p[parameter.Name] = parameter
	return nil
}

// NewQueryLocation creates a query location
func NewQueryLocation(name string) *Location {
	return &Location{Name: name, Kind: KindQuery}
}

// NewBodyLocation creates a body location
func NewBodyLocation(name string) *Location {
	return &Location{Name: name, Kind: KindRequestBody}
}

// NewDataViewLocation creates a dataview location
func NewDataViewLocation(name string) *Location {
	return &Location{Name: name, Kind: KindDataView}
}

func NewConstLocation(name string) *Location {
	return &Location{Kind: KindLiteral, Name: name}
}

// NewPathLocation creates a structql
func NewPathLocation(name string) *Location {
	return &Location{Name: name, Kind: KindPath}
}

// WithParameterType returns schema type parameter option
func WithParameterType(t reflect.Type) ParameterOption {
	return func(p *Parameter) {
		switch t.Kind() {
		case reflect.String, reflect.Int, reflect.Float64, reflect.Float32, reflect.Bool:
			p.Schema = &Schema{DataType: t.Kind().String()}
			return
		}
		p.Schema = NewSchema(t)
	}
}

// NewRefParameter creates a new ref parameter
func NewRefParameter(name string) *Parameter {
	return &Parameter{Reference: shared.Reference{Ref: name}}
}

// NewParameter creates a parameter
func NewParameter(name string, in *Location, opts ...ParameterOption) *Parameter {
	ret := &Parameter{Name: name, In: in}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
