package warmup

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/service/reader"
	errUtils "github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/read/cache"
	"strings"
	"sync"
	"time"
)

type (
	matchersCollector struct {
		size     int
		matchers []*cache.ParmetrizedQuery
		mux      sync.Mutex
		builder  *reader.Builder
		view     *view.View
	}

	warmupEntry struct {
		matcher *cache.ParmetrizedQuery
		view    *view.View
		column  string
		label   string
	}

	warmupEntryFn func() (*warmupEntry, error)
	notifierFn    func() (int, error)

	EntryResult struct {
		View      string
		Column    string
		Params    string
		Elapsed   string
		TimeTaken time.Duration
		Rows      int
	}

	Result struct {
		Rows    int
		Entries []*EntryResult
	}
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
	started := time.Now()
	cacheCases, err := c.view.Cache.GenerateCacheInput(ctx)
	if err != nil {
		fmt.Printf("[INFO] cache warmup selector error view=%s cache=%s elapsed=%s error=%v\n", c.view.Name, cacheLabel(c.view), time.Since(started), err)
		return 0, err
	}
	fmt.Printf("[INFO] cache warmup selector done view=%s cache=%s cases=%d elapsed=%s\n", c.view.Name, cacheLabel(c.view), len(cacheCases), time.Since(started))

	for i := range cacheCases {
		go c.populateChan(ctx, c.view, collector, cacheCases[i])
	}

	cacheSize := len(cacheCases)
	for _, cacheCase := range cacheCases {
		if cacheCase.IndexMeta {
			cacheSize++
		}
	}

	return cacheSize, err
}

func (c *matchersCollector) populateChan(ctx context.Context, aView *view.View, aChan chan warmupEntryFn, cacheInput *view.CacheInput) {
	c.createIndexWarmupEntry(ctx, aView, aChan, cacheInput)

	if !cacheInput.IndexMeta {
		return
	}

	c.createMetaWarmupEntry(ctx, aView, aChan, cacheInput)
}

func (c *matchersCollector) createMetaWarmupEntry(ctx context.Context, aView *view.View, aChan chan warmupEntryFn, input *view.CacheInput) {
	cacheIndex, err := c.builder.CacheMetaSQL(ctx, aView, input.Selector, nil, nil, nil)
	if err != nil {
		fmt.Printf("[INFO] cache warmup entry build error view=%s type=meta column=%s error=%v\n", aView.Name, input.MetaColumn, err)
		aChan <- func() (*warmupEntry, error) {
			return nil, err
		}
		return
	}

	aChan <- func() (*warmupEntry, error) {
		return &warmupEntry{
			matcher: cacheIndex,
			view:    aView,
			column:  input.MetaColumn,
			label:   input.Label,
		}, nil
	}
}

func (c *matchersCollector) createIndexWarmupEntry(ctx context.Context, aView *view.View, aChan chan warmupEntryFn, cacheInput *view.CacheInput) {
	build, err := c.builder.CacheSQL(ctx, c.view, cacheInput.Selector)
	if err != nil {
		fmt.Printf("[INFO] cache warmup entry build error view=%s type=index column=%s error=%v\n", aView.Name, cacheInput.Column, err)
	}
	aChan <- func() (*warmupEntry, error) {
		if err != nil {
			return nil, err
		}

		return &warmupEntry{
			matcher: build,
			view:    aView,
			column:  cacheInput.Column,
			label:   cacheInput.Label,
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

func warmup(ctx context.Context, entries []*warmupEntry, notifier chan func() (*EntryResult, error)) {
	for i := range entries {
		go readWithChan(ctx, entries[i], notifier)
	}
}

func readWithChan(ctx context.Context, entry *warmupEntry, notifier chan func() (*EntryResult, error)) {
	result, err := readWithErr(ctx, entry)
	notifier <- func() (*EntryResult, error) {
		return result, err
	}
}

func readWithErr(ctx context.Context, entry *warmupEntry) (*EntryResult, error) {
	started := time.Now()
	fmt.Printf("[INFO] cache warmup query start start_time=%s view=%s cache=%s db_connector=%s column=%s params=%s args=%v sql=%q\n", started.Format(time.RFC3339), entry.view.Name, cacheLabel(entry.view), warmupConnectorLabel(entry.view), entry.column, entry.label, entry.matcher.Args, truncateSQL(entry.matcher.SQL))
	db, err := DB(entry)
	if err != nil {
		fmt.Printf("[INFO] cache warmup query error view=%s column=%s params=%s elapsed=%s error=%v\n", entry.view.Name, entry.column, entry.label, time.Since(started), err)
		return nil, err
	}

	service, err := entry.view.Cache.Service()
	if err != nil {
		fmt.Printf("[INFO] cache warmup query error view=%s column=%s params=%s elapsed=%s error=%v\n", entry.view.Name, entry.column, entry.label, time.Since(started), err)
		return nil, err
	}

	matcher := entry.matcher
	indexed, err := service.IndexBy(ctx, db, entry.column, matcher.SQL, matcher.Args)
	elapsed := time.Since(started)
	if err != nil {
		fmt.Printf("[INFO] cache warmup query error view=%s column=%s params=%s rows=%d elapsed=%s error=%v\n", entry.view.Name, entry.column, entry.label, indexed, elapsed, err)
		return &EntryResult{View: entry.view.Name, Column: entry.column, Params: entry.label, Elapsed: elapsed.String(), TimeTaken: elapsed, Rows: indexed}, fmt.Errorf("failed to index: %w, %v", err, matcher.SQL)
	}

	fmt.Printf("[INFO] cache warmup query done view=%s cache=%s db_connector=%s column=%s params=%s rows=%d elapsed=%s\n", entry.view.Name, cacheLabel(entry.view), warmupConnectorLabel(entry.view), entry.column, entry.label, indexed, elapsed)
	return &EntryResult{View: entry.view.Name, Column: entry.column, Params: entry.label, Elapsed: elapsed.String(), TimeTaken: elapsed, Rows: indexed}, nil
}

func DB(entry *warmupEntry) (*sql.DB, error) {
	if entry.view.Cache.Warmup.Connector != nil {
		return entry.view.Cache.Warmup.Connector.DB()
	}

	return entry.view.Db()
}

func PopulateCache(views []*view.View) (int, error) {
	result, err := PopulateCacheWithDetails(views)
	if result == nil {
		return 0, err
	}
	return result.Rows, err
}

func PopulateCacheWithDetails(views []*view.View) (*Result, error) {
	started := time.Now()
	viewsWithCache := FilterCacheViews(views)
	fmt.Printf("[INFO] cache warmup populate start start_time=%s views=%s cache_views=%s cache_count=%d\n", started.Format(time.RFC3339), namesOf(views), namesOf(viewsWithCache), len(viewsWithCache))

	if len(viewsWithCache) == 0 {
		fmt.Printf("[INFO] cache warmup populate done rows=0 elapsed=%s\n", time.Since(started))
		return &Result{}, nil
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
		fmt.Printf("[INFO] cache warmup populate done rows=0 entries=0 elapsed=%s\n", time.Since(started))
		return &Result{}, nil
	}
	fmt.Printf("[INFO] cache warmup entries expected entries=%d elapsed=%s\n", collectorSize, time.Since(started))

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
	if err := errUtils.CombineErrors("errors while populating cache: ", errors); err != nil {
		fmt.Printf("[INFO] cache warmup populate error entries=%d elapsed=%s error=%v\n", len(warmupEntries), time.Since(started), err)
		return &Result{}, err
	}
	fmt.Printf("[INFO] cache warmup entries built entries=%d elapsed=%s\n", len(warmupEntries), time.Since(started))

	notifierErr := make(chan func() (*EntryResult, error))
	warmup(ctx, warmupEntries, notifierErr)
	result := &Result{}

	for i := 0; i < len(warmupEntries); i++ {
		select {
		case actual := <-notifierErr:
			if actual != nil {
				entryResult, err := actual()
				if entryResult != nil {
					result.Rows += entryResult.Rows
					result.Entries = append(result.Entries, entryResult)
				}
				if err != nil {
					errors = append(errors, err)
				}
			}
		}
	}

	close(notifier)
	err := errUtils.CombineErrors("errors while populating cache: ", errors)
	if err != nil {
		fmt.Printf("[INFO] cache warmup populate error rows=%d entries=%d elapsed=%s error=%v\n", result.Rows, len(warmupEntries), time.Since(started), err)
		return result, err
	}
	fmt.Printf("[INFO] cache warmup populate done rows=%d entries=%d elapsed=%s\n", result.Rows, len(warmupEntries), time.Since(started))
	return result, nil
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

func namesOf(views []*view.View) string {
	if len(views) == 0 {
		return ""
	}
	names := make([]string, 0, len(views))
	for _, candidate := range views {
		if candidate == nil {
			continue
		}
		names = append(names, candidate.Name)
	}
	return strings.Join(names, ",")
}

func cacheLabel(aView *view.View) string {
	if aView == nil || aView.Cache == nil {
		return ""
	}
	if aView.Cache.Name != "" {
		return aView.Cache.Name
	}
	return aView.Cache.Provider
}

func warmupConnectorLabel(aView *view.View) string {
	if aView == nil || aView.Cache == nil || aView.Cache.Warmup == nil || aView.Cache.Warmup.Connector == nil {
		return viewConnectorLabel(aView)
	}
	return connectorLabel(aView.Cache.Warmup.Connector)
}

func viewConnectorLabel(aView *view.View) string {
	if aView == nil || aView.Connector == nil {
		return ""
	}
	return connectorLabel(aView.Connector)
}

func connectorLabel(connector *view.Connector) string {
	if connector == nil {
		return ""
	}
	if connector.Ref != "" {
		return connector.Ref
	}
	if connector.Name != "" {
		return connector.Name
	}
	if connector.Driver != "" {
		return connector.Driver
	}
	return ""
}

func truncateSQL(SQL string) string {
	SQL = strings.Join(strings.Fields(SQL), " ")
	if len(SQL) <= 512 {
		return SQL
	}
	return SQL[:512] + "...(truncated)"
}
