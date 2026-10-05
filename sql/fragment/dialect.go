package fragment

import (
	"context"
	"github.com/viant/datly/internal/dialectcontext"

	"github.com/viant/sqlx/metadata/info"
)

// WithDialect scopes pure SQL rendering metadata to nested predicate
// evaluation. It carries no database/transaction handle or execution service.
func WithDialect(ctx context.Context, dialect *info.Dialect) context.Context {
	return dialectcontext.WithDialect(ctx, dialect)
}

func Dialect(ctx context.Context) *info.Dialect {
	return dialectcontext.Dialect(ctx)
}
