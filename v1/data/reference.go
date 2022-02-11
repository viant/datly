package data

import (
	"context"
	"fmt"
	"github.com/viant/datly/v1/config"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"strings"
)

//Relation represents  data View reference
type Relation struct {
	Name string
	Of   *RelationReference

	Cardinality    string //One, or Many
	Column         string //event_Type_id, employee#id
	Holder         string //holderField holding ref,
	IncludeColumn  bool   //generate column to parent view component
	HasColumnField bool

	holderField *xunsafe.Field
	columnField *xunsafe.Field
}

// Employee -> []Deps
// Column -> Employee#Id

type RelationReference struct {
	View          // event type
	Column string // EventType.id
	field  *xunsafe.Field
}

func (r *RelationReference) Init(ctx context.Context, views Views, connectors config.Connectors, types Types) error {
	if r.View.Ref != "" {
		view, err := views.Lookup(r.View.Ref)
		if err != nil {
			return err
		}

		if err := view.Init(ctx, views, connectors, types); err != nil {
			return err
		}
		r.View.inherit(view)
	}

	r2 := r.Component.Type()
	r.field = xunsafe.FieldByName(r2, r.Caser.Format(r.Column, format.CaseUpperCamel))
	return r.Validate()
}

func (r *RelationReference) Validate() error {
	if r.Column == "" {
		return fmt.Errorf("reference column can't be empty")
	}
	return nil
}

func (r *Relation) Init(ctx context.Context, views Views, connectors config.Connectors, types Types) error {
	if err := r.Of.Init(ctx, views, connectors, types); err != nil {
		return err
	}
	return r.Validate()
}

func (r *Relation) Validate() error {
	if r.Cardinality != "Many" && r.Cardinality != "One" {
		return fmt.Errorf("cardinality has to be Many or One")
	}

	if r.Column == "" {
		return fmt.Errorf("column can't be empty")
	}

	if r.Of == nil {
		return fmt.Errorf("relation of can't be nil")
	}

	if r.Holder == "" {
		return fmt.Errorf("refHolder can't be empty")
	}

	if strings.Title(r.Holder)[0] != r.Holder[0] {
		return fmt.Errorf("holder has to start with uppercase")
	}

	if r.Of.field == nil {
		return fmt.Errorf("could not fount holderField with name: %v", r.Of.Caser.Format(r.Column, format.CaseUpperCamel))
	}
	return nil
}
