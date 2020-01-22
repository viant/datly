package data

import (
	"context"
	"datly/generic"
	"datly/visitor"
)

type Visitor struct {
	Visitor string
	_visit  visitor.Visit
}

//Visitors represents visitors
type Visitors []*Visitor

//Init initialises visitors
func (v *Visitor) Init() error {
	var err error
	v._visit, err = visitor.Registry().Get(v.Visitor)
	return err
}

//Visit visit an object
func (v *Visitor) Visit(ctx context.Context, object *generic.Object) (bool, error) {
	return v._visit(ctx, object)
}
