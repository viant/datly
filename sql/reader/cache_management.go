package reader

import (
	"context"
	"fmt"
	"sort"

	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
)

func (e *Execution) CacheViews() []string {
	var result []string
	for v, c := range e.config.ReadCaches {
		if c != nil {
			result = append(result, viewName(v))
		}
	}
	sort.Strings(result)
	return result
}
func (e *Execution) InvalidateCache(ctx context.Context, view, scope string) ([]dexec.CacheInvalidation, error) {
	if err := dexec.ValidateCacheScope(scope); err != nil {
		return nil, err
	}
	var views []*data.View
	if view != "" {
		resolved, err := e.config.Plan.ViewIndex.Resolve(view)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", dexec.ErrCacheViewNotFound, err)
		}
		if e.config.ReadCaches[resolved] == nil {
			return nil, fmt.Errorf("%w: %s", dexec.ErrCacheViewNotFound, view)
		}
		views = append(views, resolved)
	} else {
		for v, c := range e.config.ReadCaches {
			if c != nil {
				views = append(views, v)
			}
		}
	}
	if len(views) == 0 {
		return nil, fmt.Errorf("component has no cached views")
	}
	sort.Slice(views, func(i, j int) bool { return viewName(views[i]) < viewName(views[j]) })
	type invalidator = dexec.CacheServiceInvalidator
	// Preflight every provider before making changes.
	for _, v := range views {
		if _, ok := e.config.ReadCaches[v].(invalidator); !ok {
			return nil, fmt.Errorf("cache for view %q does not support scoped invalidation", viewName(v))
		}
	}
	var result []dexec.CacheInvalidation
	var failure error
	for _, v := range views {
		token, err := e.config.ReadCaches[v].(invalidator).InvalidateCache(ctx, scope)
		item := dexec.CacheInvalidation{View: viewName(v), Scope: scope, Generation: token}
		if err != nil {
			item.Error = err.Error()
			failure = err
		}
		result = append(result, item)
	}
	return result, failure
}
