package reader

import "context"

// OnFetcher lifecycle interface that if entity implements, OnFetch(ctx context.NormalizeObject) is call after record is fetched from db
type OnFetcher interface {
	OnFetch(ctx context.Context) error
}

// OnRelationer lifecycle interface that if entity implements, OnRelation(ctx context.NormalizeObject) is call after relation is assembled
type OnRelationer interface {
	OnRelation(ctx context.Context)
}

type OnRelationerConcurrency interface {
	Concurrency() int
}
