package data

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
)

type (
	//Parameter describes parameters used by the Criteria
	Parameter struct {
		shared.Reference
		Name            string
		In              *Location
		Required        *bool
		Description     string
		Style           string
		AllowEmptyValue bool
		Schema          Schema

		initialized bool
		view        *View
	}

	Location struct {
		Kind Kind
		Name string
	}
)

//Init initializes Parameter
func (p *Parameter) Init(ctx context.Context, resource *Resource) error {
	if p.initialized == true {
		return nil
	}

	p.initialized = true
	if p.Ref != "" && p.Name == "" {
		param, err := resource._parameters.Lookup(p.Ref)
		if err != nil {
			return err
		}

		if err = param.Init(ctx, resource); err != nil {
			return err
		}

		p.inherit(param)

		if p.In.Kind == DataViewKind {
			view, err := resource.View(p.In.Name)
			if err != nil {
				return fmt.Errorf("failed to lookup parameter %v view %w", p.Name, err)
			}

			if err = view.Init(ctx, resource); err != nil {
				return err
			}

			p.view = view
			p.view.ParamField = xunsafe.FieldByName(p.view.DataType(), view.Caser.Format(view.Columns[0].Name, format.CaseUpperCamel))
		}
	}

	return p.Validate()
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
