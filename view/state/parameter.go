package state

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/shared"
	"github.com/viant/structology"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type (
	Parameter struct {
		shared.Reference
		Fields    Parameters
		Predicate *config.PredicateConfig
		Name      string `json:",omitempty"`

		In                *Location   `json:",omitempty"`
		Required          *bool       `json:",omitempty"`
		Description       string      `json:",omitempty"`
		DataType          string      `json:",omitempty"`
		Style             string      `json:",omitempty"`
		MaxAllowedRecords *int        `json:",omitempty"`
		MinAllowedRecords *int        `json:",omitempty"`
		ExpectedReturned  *int        `json:",omitempty"`
		Schema            *Schema     `json:",omitempty"`
		Output            *Codec      `json:",omitempty"`
		Const             interface{} `json:",omitempty"`
		DateFormat        string      `json:",omitempty"`
		ErrorStatusCode   int         `json:",omitempty"`

		selector     *structology.Selector
		_initialized bool
	}

	LookupParameter func(name string) (*Parameter, error)

	LookupSchema func(ctx context.Context, schema string) (*Schema, error)

	Codec struct {
		shared.Reference
		Name      string           `json:",omitempty"`
		Signature []*NamedArgument //inherited from registry
		Args      []string

		config.CodecConfig
		Schema *Schema `json:",omitempty"`
	}

	NamedArgument struct {
		Name     string
		Position int
	}
)

func (v *Parameter) OutputSchema() *Schema {
	if v.Output != nil && v.Output.Schema != nil {
		return v.Output.Schema
	}
	return v.Schema
}

// Init initializes Parameter
func (p *Parameter) Init(ctx context.Context, parameters LookupParameter, viewSchema LookupSchema, typeLookup xreflect.LookupType) error {
	if p._initialized == true {
		return nil
	}
	p._initialized = true

	if p.In == nil {
		return fmt.Errorf("parameter %v In can't be empty", p.Name)
	}
	if p.Ref != "" {
		ref, err := parameters(p.Ref)
		if err != nil {
			return fmt.Errorf("invalid parameter ref: %v, %w", p.Ref, err)
		}
		p.inherit(ref)
	}
	p.In.Kind = Kind(strings.ToLower(string(p.In.Kind)))
	if p.In.Kind == KindLiteral && p.Const == nil {
		return fmt.Errorf("param %v value was not set", p.Name)
	}

	/*	if err := p.initSchema(resource, stateType); err != nil {
			return err
		}
	*/
	return p.Validate()
}

func (p *Parameter) initSchema(stateType reflect.Type) error {
	/*
		if p.In.Kind == KindRequest {
			p.Schema = NewSchema(reflect.TypeOf(&http.Request{}))
			return nil
		}

		if p.Schema == nil {
			if p.In.Kind == KindLiteral {
				p.Schema = NewSchema(reflect.TypeOf(p.Const))
			} else if p.In.Kind == KindRequest {
				p.Schema = NewSchema(reflect.TypeOf(&http.Request{}))
			} else {
				return fmt.Errorf("parameter %v schema can't be empty", p.Name)
			}
		}

		if p.Schema.Type() != nil {
			return nil
		}

		if stateType != nil {
			return p.initSchemaFromType(stateType)
		}

		if p.In.Kind == KindLiteral {
			p.Schema = NewSchema(reflect.TypeOf(p.Const))
			return nil
		}

		if p.Schema == nil {
			if p.DataType != "" {
				p.Schema = &Schema{DataType: p.DataType}
			} else {
				return fmt.Errorf("parameter %v schema can't be empty", p.Name)
			}
		}

		if p.Schema.DataType == "" && p.Schema.Name == "" {
			return fmt.Errorf("parameter %v either schema Type or Name has to be specified", p.Name)
		}

		schemaType := FirstNotEmpty(p.Schema.Name, p.Schema.DataType)
		if p.MaxAllowedRecords != nil && *p.MaxAllowedRecords > 1 {
			p.Schema.Cardinality = Many
		}

		if schemaType != "" {
			lookup, err := types.LookupType(resource.LookupType(), schemaType)
			if err != nil {
				return err
			}

			p.Schema.SetType(lookup)
			return nil

		}

		return p.Schema.Init(resource, 0)
	*/
	return nil
}

func (p *Parameter) inherit(param *Parameter) {
	setter.SetStringIfEmpty(&p.Name, param.Name)
	setter.SetStringIfEmpty(&p.Description, param.Description)
	setter.SetStringIfEmpty(&p.Style, param.Style)
	if p.Const == nil {
		p.Const = param.Const
	}

	if p.In == nil {
		p.In = param.In
	}

	if p.Required == nil {
		p.Required = param.Required
	}

	if p.Schema == nil {
		p.Schema = param.Schema.Clone()
	}

	if p.Output == nil {
		p.Output = param.Output
	}

	if p.ErrorStatusCode == 0 {
		p.ErrorStatusCode = param.ErrorStatusCode
	}

	if p.Predicate == nil {
		p.Predicate = param.Predicate
	}
}

// Validate checks if parameter is valid
func (p *Parameter) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("parameter name can't be empty")
	}
	if p.In == nil {
		return fmt.Errorf("parameter location can't be empty")
	}
	if err := p.In.Validate(); err != nil {
		return err
	}
	return nil
}

func (p *Parameter) IsRequired() bool {
	return p.Required != nil && *p.Required == true
}
