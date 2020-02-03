package cache

import (
	"context"
	"time"
)

//Service represents generic cache service
type Service interface {
	//Put write key to cache
	Put(ctx context.Context, key string, data []byte, ttl time.Duration) error
	//Get cache value
	Get(ctx context.Context, key string) ([]byte,  error)
	//Remove cache for supplied key
	Delete(ctx context.Context, key string) error
}
