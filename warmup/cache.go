package warmup

import (
	"context"
	"fmt"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/read/cache"
	"sync"
)

type (
	matchersCollector struct {
		size     int
		matchers []*cache.Index
		mux      sync.Mutex
		builder  *reader.Builder
		view     *view.View
	}

	warmupEntry struct {
		matcher *cache.Index
		view    *view.View
		column  string
	}

	warmupEntryFn func() (*warmupEntry, error)
	notifierFn    func() (int, error)
)

func (c *matchersCollector) populate(ctx context.Context, collector chan warmupEntryFn, notifier chan notifierFn) {
	go func() {
		size, err := c.populateCacheCases(ctx, collector)

		notifier <- func() (int, error) {
			return size, err
		}
	}()
}

func (c *matchersCollector) populateCacheCases(ctx context.Context, collector chan warmupEntryFn) (int, error) {
	cacheCases, err := c.view.Cache.GenerateCacheInput(ctx)
	if err != nil {
		return 0, err
	}

	for i := range cacheCases {
		go c.populateChan(c.view, collector, cacheCases[i])
	}

	return len(cacheCases), err
}

func (c *matchersCollector) populateChan(aView *view.View, aChan chan warmupEntryFn, cacheInput *view.CacheInput) {
	build, err := c.builder.Build(c.view, cacheInput.Selector, &view.BatchData{}, nil, &reader.Exclude{
		ColumnsIn:  true,
		Pagination: true,
	}, nil)

	aChan <- func() (*warmupEntry, error) {
		if err != nil {
			return nil, err
		}

		return &warmupEntry{
			matcher: build,
			view:    aView,
			column:  cacheInput.Column,
		}, err
	}
}

func populateCollector(ctx context.Context, aView *view.View, builder *reader.Builder, collector chan warmupEntryFn, notifier chan notifierFn) {
	(&matchersCollector{
		size:     0,
		matchers: nil,
		view:     aView,
		builder:  builder,
		mux:      sync.Mutex{},
	}).populate(ctx, collector, notifier)
}

func warmup(ctx context.Context, entries []*warmupEntry, notifier chan error) {
	for i := range entries {
		go readWithChan(ctx, entries[i], notifier)
	}
}

func readWithChan(ctx context.Context, entry *warmupEntry, notifier chan error) {
	notifier <- readWithErr(ctx, entry)
}

func readWithErr(ctx context.Context, entry *warmupEntry) error {
	db, err := entry.view.Db()
	if err != nil {
		return err
	}

	service, err := entry.view.Cache.Service()
	if err != nil {
		return err
	}

	matcher := entry.matcher
	if err = service.IndexBy(ctx, db, entry.column, matcher.SQL, matcher.Args); err != nil {
		return err
	}

	return nil
}

func PopulateCache(views []*view.View) (int, error) {
	viewsWithCache := FilterCacheViews(views)

	if len(viewsWithCache) == 0 {
		return 0, nil
	}

	collector := make(chan warmupEntryFn)
	notifier := make(chan notifierFn)
	ctx := context.Background()

	builder := reader.NewBuilder()
	for i := range viewsWithCache {
		populateCollector(ctx, viewsWithCache[i], builder, collector, notifier)
	}

	counter := 0
	collectorSize := 0
	for counter < len(viewsWithCache) {
		select {
		case fn := <-notifier:
			chunkSize, err := fn()
			collectorSize += chunkSize

			if err != nil {
				fmt.Printf("encounter err while creating selectors: %v\n", err.Error())
			}

			counter++
		}
	}

	if collectorSize == 0 {
		return 0, nil
	}

	var errors []error
	var warmupEntries []*warmupEntry
	var collectorsCounter int
	for fn := range collector {
		entry, err := fn()
		if err != nil {
			errors = append(errors, err)
		} else {
			warmupEntries = append(warmupEntries, entry)
		}

		collectorsCounter++
		if collectorSize <= collectorsCounter {
			break
		}
	}

	close(collector)
	if err := combineErrors(errors); err != nil {
		return 0, err
	}

	notifierErr := make(chan error)
	warmup(ctx, warmupEntries, notifierErr)
	for i := 0; i < len(warmupEntries); i++ {
		select {
		case actual := <-notifierErr:
			if actual != nil {
				errors = append(errors, actual)
			}
		}
	}

	close(notifier)
	return collectorsCounter, combineErrors(errors)
}

func FilterCacheViews(views []*view.View) []*view.View {
	viewsWithCache := make([]*view.View, 0)

	for i, aView := range views {
		if aView.Cache != nil && aView.Cache.Warmup != nil {
			viewsWithCache = append(viewsWithCache, views[i])
		}
	}

	return viewsWithCache
}

func combineErrors(errors []error) error {
	if len(errors) == 0 {
		return nil
	}

	outputErr := fmt.Errorf("errors while populating cache: ")
	for _, err := range errors {
		outputErr = fmt.Errorf("%w; %v", outputErr, err.Error())
	}

	return outputErr
}
