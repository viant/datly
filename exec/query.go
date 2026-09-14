package exec

import (
	"context"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

// QueryPreparer is an optional capability of a compiled SQL reader.
type QueryPreparer interface {
	PrepareQuery(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (*PreparedQuery, error)
}

// PreparedQuery contains bound source SQL and a projection executor owned by
// that source connection. No database or transaction is exposed to handlers.
type PreparedQuery struct {
	SQL        string
	Args       []any
	Projection ProjectionReader
}

type ProjectionRequest struct {
	SQL     string
	Args    []any
	RowType reflect.Type
}

type ProjectionReader interface {
	ReadProjection(context.Context, ProjectionRequest) (any, error)
}
