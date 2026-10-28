package exec

import (
	"context"
	"github.com/viant/sqlx"
	xh "github.com/viant/xdatly/handler"
)

// RowReader is the optional non-retaining execution capability of a compiled reader.
type RowReader interface {
	ReadRows(context.Context, any, xh.Binder, sqlx.ParameterResolver, *xh.RowConsumer) (any, error)
}
