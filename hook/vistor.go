package hook

import (
	"context"
	"datly/generic"
)

//Visitor represent an object visitor
type Visitor func(ctx context.Context, object *generic.Object) (bool, error)
