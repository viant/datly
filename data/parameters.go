package data

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/velty"
	"github.com/viant/xunsafe"
	"reflect"
)

type (
	//Parameter describes parameters used by the Criteria to filter the data.
	Parameter struct {
		shared.Reference
		Name         string
		PresenceName string

		In          *Location
		Required    *bool
		Description string
		Style       string
		Schema      *Schema

		initialized    bool
		view           *View
		xfield         *xunsafe.Field
		presenceXfield *xunsafe.Field
	}

	//Location tells how to get parameter value.
	Location struct {
		Kind Kind
		Name string
	}
)

//Init initializes Parameter
func (p *Parameter) Init(ctx context.Context, resource *Resource, xfield *xunsafe.Field) error {
	if p.initialized == true {
		return nil
	}

	if p.Ref != "" && p.Name == "" {
		param, err := resource._parameters.Lookup(p.Ref)
		if err != nil {
			return err
		}

		if err = param.Init(ctx, resource, xfield); err != nil {
			return err
		}

		p.inherit(param)
	}

	if p.In.Kind == DataViewKind {
		view, err := resource.View(p.In.Name)
		if err != nil {
			return fmt.Errorf("failed to lookup parameter %v view %w", p.Name, err)
		}

		if err = view.Init(ctx, resource); err != nil {
			return err
		}

		p.view = view
	}

	if err := p.initSchema(resource.types, xfield); err != nil {
		return err
	}

	err := p.Validate()
	if err != nil {
		return err
	}

	if p.PresenceName == "" {
		p.PresenceName = p.Name
	}

	p.xfield = xfield
	p.initialized = true
	return nil
}

func (p *Parameter) inherit(param *Parameter) {
	p.Name = notEmptyOf(p.Name, param.Name)
	p.Description = notEmptyOf(p.Description, param.Description)
	p.Style = notEmptyOf(p.Style, param.Style)

	if p.In == nil {
		p.In = param.In
	}

	if p.Required == nil {
		p.Required = param.Required
	}

	if p.Schema == nil {
		p.Schema = param.Schema.copy()
	}
}

//Validate checks if parameter is valid
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

//View returns View related with Parameter if Location.Kind is set to data_view
func (p *Parameter) View() *View {
	return p.view
}

//Validate checks if Location is valid
func (l *Location) Validate() error {
	if err := l.Kind.Validate(); err != nil {
		return err
	}

	if err := ParamName(l.Name).Validate(l.Kind); err != nil {
		return fmt.Errorf("unsupported param name")
	}

	return nil
}

func (p *Parameter) IsRequired() bool {
	return p.Required != nil && *p.Required == true
}

func (p *Parameter) initSchema(types Types, xfield *xunsafe.Field) error {
	if xfield != nil {
		return p.initSchemaFromStructType(xfield.Type)
	}

	if p.Schema == nil {
		return fmt.Errorf("parameter %v schema can't be empty", p.Name)
	}

	if p.Schema.DataType == "" {
		return fmt.Errorf("parameter %v schema DataType can't be empty", p.Name)
	}

	return p.Schema.Init(nil, nil, 0, types)
}

func (p *Parameter) initSchemaFromStructType(structType reflect.Type) error {
	if p.Schema == nil {
		p.Schema = &Schema{}
	}

	elem := shared.Elem(structType)

	structField, ok := elem.FieldByName(p.Name)
	if !ok {
		structField, ok = findFieldByTagName(elem, p.Name)
		if !ok {
			return fmt.Errorf("not found %v field at type %v", p.Name, structType.String())
		}
	}

	p.Schema.setType(structField.Type)
	p.xfield = xunsafe.FieldByName(structType, p.Name)
	return nil
}

func findFieldByTagName(elem reflect.Type, name string) (reflect.StructField, bool) {
	for i := 0; i < elem.NumField(); i++ {
		field := elem.Field(i)
		tag := velty.Parse(field.Tag.Get("velty"))
		for _, veltyName := range tag.Names {
			if veltyName == name {
				return field, true
			}
		}
	}

	return reflect.StructField{}, false
}

func (p *Parameter) Mutator() *xunsafe.Field {
	return p.xfield
}

func (p *Parameter) PresenceMutator() *xunsafe.Field {
	return p.presenceXfield
}
