package reader

import "context"

type OnFetcher interface {
	OnFetch(ctx context.Context) error
}

type OnRelationCompleter interface {
	OnRelation(ctx context.Context)
}
