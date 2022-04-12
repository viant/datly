package reader

import "context"

type OnFetcher interface {
	OnFetch(ctx context.Context) error
}

type OnRelationCompleter interface {
	OnRelationComplete(ctx context.Context)
}
