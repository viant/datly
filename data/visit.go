package data

import (
	"context"
	"github.com/viant/datly/db"
	"github.com/viant/datly/generic"
)

//Visit represent an object visitor
type Visit func(ctx context.Context, db db.Service,  view *View, object *generic.Object) (bool, error)
