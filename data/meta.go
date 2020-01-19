package data

import (
	"datly/generic"
	"fmt"
	"github.com/pkg/errors"
)

//Meta represents an abstraction describing data access rules
type Meta struct {
	Output []*Output `json:",omitempty"`
	Views  []*View   `json:",omitempty"`
}

//AssociationViews returns join views
func (r *Meta) Init() error {

	for i := range r.Views {
		view := r.Views[i]
		if len(view.Refs) > 0 {
			for i, ref := range view.Refs {
				refView, err := r.View(view.Refs[i].DataView)
				if err != nil {
					return errors.Wrapf(err, "failed to construct join: %v", view.Refs[i].Name)
				}
				refView = refView.Clone()
				refView.Joins = make([]*Join, 0)
				view.Refs[i]._view = refView
				view.Refs[i]._alias = fmt.Sprintf("a%02v", i)
				view.Refs[i]._refIndex = generic.NewIndex(ref.RefColumns())
				view.Refs[i]._index = generic.NewIndex(ref.Columns())
			}
		}
	}
	return nil
}

//Validate checks if rules are valid
func (r *Meta) Validate() error {
	if len(r.Views) == 0 {
		return errors.New("views was empty")
	}
	if len(r.Output) == 0 {
		return errors.New("outputs was empty")
	}
	for _, view := range r.Views {
		if err := view.Validate(); err != nil {
			return err
		}
	}
	for _, output := range r.Output {
		if err := output.Validate(); err != nil {
			return err
		}
	}
	return nil
}

//View returns a view for supplied name or error
func (r *Meta) View(name string) (*View, error) {
	for _, view := range r.Views {
		if view.Name == name {
			return view, nil
		}
	}
	return nil, errors.Errorf("failed to lookup view: %v", name)
}
