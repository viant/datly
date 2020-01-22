package visitor

import (
	"context"
	"github.com/viant/datly/generic"
)

//Visit represent an object visitor
type Visit func(ctx context.Context, object *generic.Object) (bool, error)
