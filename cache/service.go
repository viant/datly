package cache

import (
	"context"
	"time"
)

//Service represents generic cache service
type Service interface {
	Put(ctx context.Context, key string, data []byte, ttl time.Duration) error
	Get(ctx context.Context, key string) ([]byte, *time.Time, error)
	Delete(ctx context.Context, key string) error
}
