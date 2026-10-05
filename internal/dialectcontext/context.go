// Package dialectcontext scopes dialect metadata shared by execution and pure
// rendering owners without carrying database handles or SQL services.
package dialectcontext

import (
	"context"

	"github.com/viant/sqlx/metadata/info"
)

type key struct{}

func WithDialect(ctx context.Context, dialect *info.Dialect) context.Context {
	return context.WithValue(ctx, key{}, dialect)
}

func Dialect(ctx context.Context) *info.Dialect {
	if ctx == nil {
		return nil
	}
	value, _ := ctx.Value(key{}).(*info.Dialect)
	return value
}
