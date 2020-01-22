package data

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/base"
	"github.com/viant/datly/generic"
)

//Meta represents an abstraction describing data access rules
type Meta struct {
	Output      []*Output `json:",omitempty"`
	Views       []*View   `json:",omitempty"`
	TemplateURL string    `json:",omitempty"`
	template    *Meta
}

//Init initialises views and outputs.
func (m *Meta) Init() error {
	m.initOutput()
	return m.initViews()
}

func (m *Meta) initViews() error {
	for i := range m.Views {
		view := m.Views[i]
		if len(view.Refs) > 0 {
			for i, ref := range view.Refs {
				refView, err := m.View(view.Refs[i].DataView)
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

func (m *Meta) initOutput() {
	if len(m.Output) == 0 && len(m.Views) > 0 {
		key := m.Views[0].Table
		if key == "" {
			if key = m.Views[0].Name; key == "" {
				key = base.DefaultDataOutputKey
			}
		}
		m.Output = []*Output{
			{
				DataView: m.Views[0].Name,
				Key:      key,
			},
		}
	}
	if len(m.Output) > 0 {
		for i := range m.Output {
			m.Output[i].Init()
		}
	}
}

//Validate checks if rules are valid
func (m *Meta) Validate() error {
	if len(m.Views) == 0 {
		return errors.New("views was empty")
	}
	if len(m.Output) == 0 {
		return errors.New("outputs was empty")
	}
	for _, view := range m.Views {
		if err := view.Validate(); err != nil {
			return err
		}
	}
	for _, output := range m.Output {
		if err := output.Validate(); err != nil {
			return err
		}
	}
	return nil
}

//View returns a view for supplied name or error
func (m *Meta) View(name string) (*View, error) {
	for _, view := range m.Views {
		if view.Name == name {
			return view, nil
		}
	}
	return nil, errors.Errorf("failed to lookup view: %v", name)
}

//SetTemplate sets template
func (m *Meta) SetTemplate(template *Meta) {
	m.template = template
}

//ApplyTemplate applies template
func (m *Meta) ApplyTemplate() {
	if m.template == nil || len(m.template.Views) == 0 {
		return
	}
	if len(m.Views) == 0 {
		m.Views = make([]*View, 0)
	}

	for i, tmpl := range m.template.Views {
		view, _ := m.View(tmpl.Name)
		if view == nil {
			m.Views = append(m.Views, m.template.Views[i])
			continue
		}
		view.MergeFrom(m.template.Views[i])
	}

}
