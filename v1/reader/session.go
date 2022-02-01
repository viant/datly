package reader

import (
	"github.com/viant/datly/v1/data"
)

type Session struct {
	Dest         interface{} //  slice
	View         *data.View
	RefRelations []*data.Relation
	Selector     *data.Selector
}

func (s *Session) MergeViewWithSelector() error {
	if s.Selector == nil {
		return nil
	}

	newView, err := s.View.MergeWithSelector(s.Selector)
	if err != nil {
		return err
	}

	s.View = newView
	return err
}
