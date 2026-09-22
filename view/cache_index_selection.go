package view

import "context"

type cacheIndexSelectionKey struct{}
type cacheIndexIdentityKey struct{}

// CacheIndexSelection is the authorized index scope supplied while building a
// reader query. It is local to one request and never shared with another read.
type CacheIndexSelection struct {
	Column string
	Values []interface{}
	Denied bool
}

// WithCacheIndexSelection prepares a request-local slot that predicate handlers
// can fill before the indexed cache matcher is constructed.
func WithCacheIndexSelection(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheIndexSelectionKey{}, &CacheIndexSelection{})
}

// SelectCacheIndex supplies the authorized values for one configured warmup
// index. An empty selection leaves the regular cache matching behavior intact.
func SelectCacheIndex(ctx context.Context, column string, values []interface{}) {
	if selection, ok := ctx.Value(cacheIndexSelectionKey{}).(*CacheIndexSelection); ok && selection != nil {
		selection.Denied = false
		selection.Column = column
		selection.Values = append(selection.Values[:0], values...)
	}
}

// DenyCacheIndex prevents a request with no authorized IDs from using the
// original untrusted index parameter to read a populated cache entry.
func DenyCacheIndex(ctx context.Context) {
	if selection, ok := ctx.Value(cacheIndexSelectionKey{}).(*CacheIndexSelection); ok && selection != nil {
		selection.Denied = true
		selection.Column = ""
		selection.Values = nil
	}
}

func IsCacheIndexDenied(ctx context.Context) bool {
	selection, _ := ctx.Value(cacheIndexSelectionKey{}).(*CacheIndexSelection)
	return selection != nil && selection.Denied
}

// SelectedCacheIndex returns the index scope supplied for this request.
func SelectedCacheIndex(ctx context.Context) *CacheIndexSelection {
	selection, _ := ctx.Value(cacheIndexSelectionKey{}).(*CacheIndexSelection)
	if selection == nil || selection.Denied || selection.Column == "" || len(selection.Values) == 0 {
		return nil
	}
	return selection
}

// WithCacheIndexIdentity marks SQL generation for a cache matcher. Unlike a
// cache warmup, this does not grant authorization bypass for the request.
func WithCacheIndexIdentity(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheIndexIdentityKey{}, true)
}

func IsCacheIndexIdentity(ctx context.Context) bool {
	value, _ := ctx.Value(cacheIndexIdentityKey{}).(bool)
	return value
}
