package data

import (
	"context"
	"fmt"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"strings"
)

//Relation represents  data View reference
type (
	Relation struct {
		Name string
		Of   *ReferenceView

		Cardinality    string //One, or Many
		Column         string //event_Type_id, employee#id
		Holder         string //holderField holding ref,
		IncludeColumn  bool   //tells if Column field should be in the struct type.
		HasColumnField bool

		holderField *xunsafe.Field
		columnField *xunsafe.Field
	}

	ReferenceView struct {
		View          // event type
		Column string // EventType.id
		field  *xunsafe.Field
	}

	MatchStrategy string
)

func (s MatchStrategy) Validate() error {
	switch s {
	case ReadAll, ReadMatched, ReadDerived:
		return nil
	}
	return fmt.Errorf("unsupported match strategy %v", s)
}

func (s MatchStrategy) SupportsParallel() bool {
	return s == ReadAll
}

const (
	ReadAll     MatchStrategy = "read_all"     // read all and later we match on backend side
	ReadMatched MatchStrategy = "read_matched" // read parent data and then filter id to match with the current view
	ReadDerived MatchStrategy = "read_derived" // use parent sql selector to add criteria to the relation view, this can only work if the connector of the relation view and parent view is the same
)

//Init initializes ReferenceView
func (r *ReferenceView) Init(ctx context.Context, resource *Resource) error {
	if r.View.Ref != "" {
		view, err := resource._views.Lookup(r.View.Ref)
		if err != nil {
			return err
		}

		if err := view.Init(ctx, resource); err != nil {
			return err
		}
		r.View.inherit(view)
	}

	r2 := r.Schema.Type()
	r.field = xunsafe.FieldByName(r2, r.Caser.Format(r.Column, format.CaseUpperCamel))
	return r.Validate()
}

//Validate checks if ReferenceView is valid
func (r *ReferenceView) Validate() error {
	if r.Column == "" {
		return fmt.Errorf("reference column can't be empty")
	}
	return nil
}

//Init initializes Relation
func (r *Relation) Init(ctx context.Context, resource *Resource) error {
	if err := r.Of.Init(ctx, resource); err != nil {
		return err
	}

	return r.Validate()
}

//Validate checks if Relation is valid
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
