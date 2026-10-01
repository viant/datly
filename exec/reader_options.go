package exec

import (
	"context"
	sqlxread "github.com/viant/sqlx/io/read"
)

// ReaderOptions are trusted invocation controls, shared by nested reader work.
// Cache ownership and identity remain with SQLX.
// Transactional views bypass shared caches, including refresh/writeback, and
// reject CacheOnly rather than silently falling back to SQL execution.
// RootView selects only the root of the invoked reader result graph.
const RootView = "$root"

type ReaderOptions struct {
	// ForUpdate requests row locks on explicitly capable views under the caller transaction.
	// These trusted controls are not bound from transport input.
	ForUpdate    []string
	RefreshCache bool
	CacheOnly    bool
	QueryScope   *sqlxread.QueryScope
}
type readerOptionsKey struct{}

func (o ReaderOptions) Context(ctx context.Context) context.Context {
	o.ForUpdate = append([]string(nil), o.ForUpdate...)
	return context.WithValue(ctx, readerOptionsKey{}, o)
}
func ReaderOptionsFromContext(ctx context.Context) ReaderOptions {
	options, _ := ctx.Value(readerOptionsKey{}).(ReaderOptions)
	return options
}
