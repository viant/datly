package exec

import (
	"context"
	"reflect"

	xhandler "github.com/viant/xdatly/handler"
)

const PredicateVariable = "predicate"

// PredicateEvaluator supplies a typed template context without exposing the
// concrete predicate implementation to SQL template compilation.
type PredicateEvaluator interface {
	ContextType() reflect.Type
	NewContext(context.Context, xhandler.Binder, func(...any)) (any, error)
}
