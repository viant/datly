package data

import (
	"reflect"
)

type Session struct {
	Dest          interface{} //  slice
	dest          []interface{}
	View          *View
	Selectors     Selectors
	AllowUnmapped bool
	relations     []*Relation
}

func (s *Session) DataType() reflect.Type {
	return s.View.DataType()
}

func (s *Session) Init() {
	s.Selectors.Init()
}

func (s *Session) ViewsDest() []interface{} {
	return s.dest
}

func (s *Session) Allocate() {
	destCount := s.View.DestCount()
	s.dest = make([]interface{}, destCount)
	s.relations = make([]*Relation, destCount)
	s.updateRelations(s.View.With)
}

func (s *Session) updateRelations(relations []*Relation) {
	if len(relations) == 0 {
		return
	}

	for i := range relations {
		relation := relations[i]
		s.relations[relation.Of.destIndex] = relation
		s.updateRelations(relation.Of.With)
	}
}

func (s *Session) RelationOwner(view *View) *Relation {
	return s.relations[view.destIndex]
}
