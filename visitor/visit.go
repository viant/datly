package visitor

import (
	"context"
	"datly/generic"
)

//Visit represent an object visitor
type Visit func(ctx context.Context, object *generic.Object) (bool, error)
