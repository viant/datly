package exec

import (
	"context"
	sqlxread "github.com/viant/sqlx/io/read"
)

// ReaderOptions are trusted invocation controls, shared by nested reader work.
// Cache ownership and identity remain with SQLX.
type ReaderOptions struct {
	RefreshCache bool
	CacheOnly    bool
	QueryScope   *sqlxread.QueryScope
}
type readerOptionsKey struct{}

func (o ReaderOptions) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, readerOptionsKey{}, o)
}
func ReaderOptionsFromContext(ctx context.Context) ReaderOptions {
	options, _ := ctx.Value(readerOptionsKey{}).(ReaderOptions)
	return options
}
