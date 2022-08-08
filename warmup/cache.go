package warmup

import (
	"context"
	"fmt"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view"
	"sync"
)

type warmupper struct {
	selectors []*view.CacheInput
	aView     *view.View
	builder   *reader.Builder
}

func (w *warmupper) warmup(ctx context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(len(w.selectors))
	for i := range w.selectors {
		//go func(data *view.CacheInput) {
		func(data *view.CacheInput) {
			defer wg.Done()
			build, err := w.builder.Build(w.aView, data.Selector, &view.BatchData{}, nil, &reader.Exclude{
				ColumnsIn:  true,
				Pagination: true,
			}, nil)
			panicOnError(err)

			service, err := w.aView.Cache.Service()
			panicOnError(err)

			db, err := w.aView.Db(ctx)
			panicOnError(err)

			err = service.IndexBy(ctx, db, data.Column, build.RawSQL, build.RawArgs)
			panicOnError(err)
		}(w.selectors[i])

		fmt.Println(i)
	}

	wg.Wait()
}

func newWarmupper(aView *view.View, selectors []*view.CacheInput) *warmupper {
	return &warmupper{
		aView:     aView,
		selectors: selectors,
		builder:   reader.NewBuilder(),
	}
}

func PopulateCache(views []*view.View) error {
	viewsWithCache := make([]*view.View, 0)

	for i, aView := range views {
		if aView.Cache != nil && aView.Cache.Warmup != nil {
			viewsWithCache = append(viewsWithCache, views[i])
		}
	}

	for i, aView := range viewsWithCache {
		cache := aView.Cache
		ctx := context.Background()
		selectors, err := cache.GenerateCacheInput(ctx)
		if err != nil {
			return err
		}

		newWarmupper(viewsWithCache[i], selectors).warmup(ctx)
	}

	return nil
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}
