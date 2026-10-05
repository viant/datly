// Package dialectcontext shares dialect metadata between predicate rendering
// and writer policy evaluation without exposing a database or transaction.
package dialectcontext

import (
	"context"

	"github.com/viant/sqlx/metadata/info"
)

type dialectKey struct{}

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
