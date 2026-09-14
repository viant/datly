package exec

import (
	"context"

	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

// Reader executes one read using input already prepared by the unified handler
// engine. SQL plans, connections, caches, and mutable reader state remain
// private to the implementation.
type Reader interface {
	Read(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (any, error)
}
