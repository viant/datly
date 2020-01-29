package data

import (
	"context"
	"github.com/viant/datly/db"
	"github.com/viant/datly/generic"
)

type Visitor struct {
	Visitor string
	_visit  Visit
}

//Init initialises visitors
func (v *Visitor) Init() error {
	var err error
	v._visit, err = VisitorRegistry().Get(v.Visitor)
	return err
}

//Visit visit an object
func (v *Visitor) Visit(ctx context.Context, db db.Service, view *View,  object *generic.Object) (bool, error) {
	return v._visit(ctx, db, view, object)
}
