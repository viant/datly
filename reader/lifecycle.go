package reader

import "context"

type OnFetcher interface {
	OnFetch(ctx context.Context) error
}

type AfterRelationCompleter interface {
	AfterRelationsComplete(ctx context.Context)
}
