package fragment

import (
	"context"

	"github.com/viant/sqlx/metadata/info"
)

type dialectKey struct{}

// WithDialect scopes pure SQL rendering metadata to nested predicate
// evaluation. It carries no database/transaction handle or execution service.
func WithDialect(ctx context.Context, dialect *info.Dialect) context.Context {
	return context.WithValue(ctx, dialectKey{}, dialect)
}

func Dialect(ctx context.Context) *info.Dialect {
	if ctx == nil {
		return nil
	}
	value, _ := ctx.Value(dialectKey{}).(*info.Dialect)
	return value
}
