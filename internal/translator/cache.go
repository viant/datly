package translator

import (
	"context"
)

func (r *Repository) ensureCache(ctx context.Context) error {
	if resource, _ := r.loadDependency(ctx, "cache.yaml"); resource != nil {
		for _, cache := range resource.CacheProviders {
			r.Caches.Append(cache)
		}
	}
	return nil

}
