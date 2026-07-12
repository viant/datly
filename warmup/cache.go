package warmup

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/service/reader"
	errUtils "github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/read/cache"
	cachehash "github.com/viant/sqlx/io/read/cache/hash"
	"strings"
	"sync"
	"time"
)

const (
	maxWarmupConcurrency = 20
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
		fields  string
		key     string
	}

	warmupEntryFn func() (*warmupEntry, error)
	notifierFn    func() (int, *EntryResult, error)

	EntryResult struct {
		View       string
		Column     string
		Params     string
		CacheKey   string
		FieldNames string
		Elapsed    string
		TimeTaken  time.Duration
		Rows       int
		Error      string `json:",omitempty"`
	}

	Result struct {
		Rows    int
		Entries []*EntryResult
	}
)

func (c *matchersCollector) populate(ctx context.Context, collector chan warmupEntryFn, notifier chan notifierFn) {
	go func() {
		size, err := c.populateCacheCases(ctx, collector)

		notifier <- func() (int, *EntryResult, error) {
			if err == nil {
				return size, nil, nil
			}
			return size, failedEntryResult(&warmupEntry{view: c.view}, 0, 0, err), err
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
		fmt.Printf("[INFO] cache warmup entry build error view=%s type=meta column=%s field_names=%s error=%v\n", aView.Name, input.MetaColumn, strings.Join(input.FieldNames, ","), err)
		aChan <- func() (*warmupEntry, error) {
			return &warmupEntry{
				view:   aView,
				column: input.MetaColumn,
				label:  input.Label,
				fields: strings.Join(input.FieldNames, ","),
			}, err
		}
		return
	}
	cacheKey, err := warmupCacheKey(cacheIndex)
	if err != nil {
		fmt.Printf("[INFO] cache warmup entry build error view=%s type=meta column=%s field_names=%s error=%v\n", aView.Name, input.MetaColumn, strings.Join(input.FieldNames, ","), err)
		aChan <- func() (*warmupEntry, error) {
			return &warmupEntry{
				view:   aView,
				column: input.MetaColumn,
				label:  input.Label,
				fields: strings.Join(input.FieldNames, ","),
			}, err
		}
		return
	}

	aChan <- func() (*warmupEntry, error) {
		return &warmupEntry{
			matcher: cacheIndex,
			view:    aView,
			column:  input.MetaColumn,
			label:   input.Label,
			fields:  strings.Join(input.FieldNames, ","),
			key:     cacheKey,
		}, nil
	}
}

func (c *matchersCollector) createIndexWarmupEntry(ctx context.Context, aView *view.View, aChan chan warmupEntryFn, cacheInput *view.CacheInput) {
	build, err := c.builder.CacheSQL(ctx, c.view, cacheInput.Selector)
	if err != nil {
		fmt.Printf("[INFO] cache warmup entry build error view=%s type=index column=%s field_names=%s error=%v\n", aView.Name, cacheInput.Column, strings.Join(cacheInput.FieldNames, ","), err)
		aChan <- func() (*warmupEntry, error) {
			return &warmupEntry{
				view:   aView,
				column: cacheInput.Column,
				label:  cacheInput.Label,
				fields: strings.Join(cacheInput.FieldNames, ","),
			}, err
		}
		return
	}
	cacheKey, err := warmupCacheKey(build)
	if err != nil {
		fmt.Printf("[INFO] cache warmup entry build error view=%s type=index column=%s field_names=%s error=%v\n", aView.Name, cacheInput.Column, strings.Join(cacheInput.FieldNames, ","), err)
		aChan <- func() (*warmupEntry, error) {
			return &warmupEntry{
				view:   aView,
				column: cacheInput.Column,
				label:  cacheInput.Label,
				fields: strings.Join(cacheInput.FieldNames, ","),
			}, err
		}
		return
	}

	aChan <- func() (*warmupEntry, error) {
		return &warmupEntry{
			matcher: build,
			view:    aView,
			column:  cacheInput.Column,
			label:   cacheInput.Label,
			fields:  strings.Join(cacheInput.FieldNames, ","),
			key:     cacheKey,
		}, nil
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
	warmupWithLimit(ctx, entries, notifier, maxWarmupConcurrency, readWithErr)
}

type warmupReadFn func(context.Context, *warmupEntry) (*EntryResult, error)

func warmupWithLimit(ctx context.Context, entries []*warmupEntry, notifier chan func() (*EntryResult, error), limit int, read warmupReadFn) {
	if len(entries) == 0 {
		return
	}
	if limit <= 0 || limit > len(entries) {
		limit = len(entries)
	}
	fmt.Printf("[INFO] cache warmup workers start entries=%d concurrency=%d\n", len(entries), limit)
	jobs := make(chan *warmupEntry)
	wg := sync.WaitGroup{}
	wg.Add(limit)
	for i := 0; i < limit; i++ {
		go func() {
			defer wg.Done()
			for entry := range jobs {
				readWithChan(ctx, entry, notifier, read)
			}
		}()
	}
	go func() {
		for _, entry := range entries {
			jobs <- entry
		}
		close(jobs)
		wg.Wait()
		close(notifier)
	}()
}

func readWithChan(ctx context.Context, entry *warmupEntry, notifier chan func() (*EntryResult, error), read warmupReadFn) {
	result, err := read(ctx, entry)
	notifier <- func() (*EntryResult, error) {
		return result, err
	}
}

func readWithErr(ctx context.Context, entry *warmupEntry) (*EntryResult, error) {
	started := time.Now()
	fmt.Printf("[INFO] cache warmup query start start_time=%s view=%s cache=%s cache_key=%s db_connector=%s column=%s params=%s field_names=%s args=%v sql=%q\n", started.Format(time.RFC3339), entry.view.Name, cacheLabel(entry.view), entry.key, warmupConnectorLabel(entry.view), entry.column, entry.label, entry.fields, entry.matcher.Args, truncateSQL(entry.matcher.SQL))
	db, err := DB(entry)
	if err != nil {
		elapsed := time.Since(started)
		fmt.Printf("[INFO] cache warmup query error view=%s cache_key=%s column=%s params=%s field_names=%s elapsed=%s cache_write=skipped error=%v\n", entry.view.Name, entry.key, entry.column, entry.label, entry.fields, elapsed, err)
		return failedEntryResult(entry, elapsed, 0, err), err
	}

	service, err := entry.view.Cache.Service()
	if err != nil {
		elapsed := time.Since(started)
		fmt.Printf("[INFO] cache warmup query error view=%s cache_key=%s column=%s params=%s field_names=%s elapsed=%s cache_write=skipped error=%v\n", entry.view.Name, entry.key, entry.column, entry.label, entry.fields, elapsed, err)
		return failedEntryResult(entry, elapsed, 0, err), err
	}

	matcher := entry.matcher
	indexed, err := service.IndexBy(indexProgressContext(ctx, entry), db, entry.column, matcher.SQL, matcher.Args, matcher)
	elapsed := time.Since(started)
	if err != nil {
		fmt.Printf("[INFO] cache warmup query error view=%s cache_key=%s column=%s params=%s field_names=%s rows=%d elapsed=%s cache_write=error error=%v\n", entry.view.Name, entry.key, entry.column, entry.label, entry.fields, indexed, elapsed, err)
		indexErr := fmt.Errorf("failed to index: %w", err)
		return failedEntryResult(entry, elapsed, indexed, indexErr), indexErr
	}

	fmt.Printf("[INFO] cache warmup query done view=%s cache=%s cache_key=%s db_connector=%s column=%s params=%s field_names=%s rows=%d elapsed=%s cache_write=success\n", entry.view.Name, cacheLabel(entry.view), entry.key, warmupConnectorLabel(entry.view), entry.column, entry.label, entry.fields, indexed, elapsed)
	return &EntryResult{View: entry.view.Name, Column: entry.column, Params: entry.label, CacheKey: entry.key, FieldNames: entry.fields, Elapsed: elapsed.String(), TimeTaken: elapsed, Rows: indexed}, nil
}

func failedEntryResult(entry *warmupEntry, elapsed time.Duration, rows int, err error) *EntryResult {
	result := &EntryResult{
		Elapsed:   elapsed.String(),
		TimeTaken: elapsed,
		Rows:      rows,
	}
	if entry != nil {
		if entry.view != nil {
			result.View = entry.view.Name
		}
		result.Column = entry.column
		result.Params = entry.label
		result.CacheKey = entry.key
		result.FieldNames = entry.fields
	}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func firstError(errors []error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func warmupCacheKey(query *cache.ParmetrizedQuery) (string, error) {
	if query == nil {
		return "", fmt.Errorf("warmup cache key query was nil")
	}
	return warmupIdentityURL(query)
}

func warmupIdentityURL(query *cache.ParmetrizedQuery) (string, error) {
	if query == nil {
		return "", fmt.Errorf("warmup identity query was nil")
	}
	SQL, _, argsMarshal, err := query.WarmupIdentity()
	if err != nil {
		return "", err
	}
	return cachehash.GenerateWithMarshal(SQL, "", "", argsMarshal)
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
	return PopulateCacheWithDetailsContext(context.Background(), views)
}

func PopulateCacheWithDetailsContext(ctx context.Context, views []*view.View) (*Result, error) {
	started := time.Now()
	viewsWithCache := FilterCacheViews(views)
	fmt.Printf("[INFO] cache warmup populate start start_time=%s views=%s cache_views=%s cache_count=%d\n", started.Format(time.RFC3339), namesOf(views), namesOf(viewsWithCache), len(viewsWithCache))
	result := &Result{}

	if len(viewsWithCache) == 0 {
		fmt.Printf("[INFO] cache warmup populate done rows=0 elapsed=%s\n", time.Since(started))
		return result, nil
	}

	collector := make(chan warmupEntryFn)
	notifier := make(chan notifierFn)

	builder := reader.NewBuilder()
	for i := range viewsWithCache {
		populateCollector(ctx, viewsWithCache[i], builder, collector, notifier)
	}

	counter := 0
	collectorSize := 0
	var errors []error
	for counter < len(viewsWithCache) {
		select {
		case fn := <-notifier:
			chunkSize, entryResult, err := fn()
			collectorSize += chunkSize
			if entryResult != nil {
				result.Entries = append(result.Entries, entryResult)
			}

			if err != nil {
				fmt.Printf("encounter err while creating selectors: %v\n", err.Error())
				errors = append(errors, err)
			}

			counter++
		}
	}

	if collectorSize == 0 {
		fmt.Printf("[INFO] cache warmup populate done rows=0 entries=0 elapsed=%s\n", time.Since(started))
		err := errUtils.CombineErrors("errors while populating cache: ", errors)
		if err != nil {
			return result, err
		}
		return result, nil
	}
	fmt.Printf("[INFO] cache warmup entries expected entries=%d elapsed=%s\n", collectorSize, time.Since(started))

	var warmupEntries []*warmupEntry
	var collectorsCounter int
	for fn := range collector {
		entry, err := fn()
		if err != nil {
			errors = append(errors, err)
			result.Entries = append(result.Entries, failedEntryResult(entry, 0, 0, err))
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
		fmt.Printf("[INFO] cache warmup populate error entries=%d failures=%d elapsed=%s first_error=%v\n", len(warmupEntries), len(errors), time.Since(started), firstError(errors))
		return result, err
	}
	fmt.Printf("[INFO] cache warmup entries built entries=%d elapsed=%s\n", len(warmupEntries), time.Since(started))

	notifierErr := make(chan func() (*EntryResult, error))
	warmup(ctx, warmupEntries, notifierErr)

	for actual := range notifierErr {
		if actual == nil {
			continue
		}
		entryResult, err := actual()
		if entryResult != nil {
			result.Entries = append(result.Entries, entryResult)
			result.Rows += entryResult.Rows
		}
		if err != nil {
			errors = append(errors, err)
		}
	}

	close(notifier)
	err := errUtils.CombineErrors("errors while populating cache: ", errors)
	if err != nil {
		fmt.Printf("[INFO] cache warmup populate error rows=%d entries=%d failures=%d elapsed=%s first_error=%v\n", result.Rows, len(warmupEntries), len(errors), time.Since(started), firstError(errors))
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

func indexProgressContext(ctx context.Context, entry *warmupEntry) context.Context {
	if entry == nil {
		return ctx
	}
	ctx = cache.WithIndexProgress(ctx, &cache.IndexProgress{
		View:    warmupViewName(entry),
		Dataset: warmupDatasetName(entry),
		Case:    entry.label,
	})
	return cache.WithIndexProgressCallback(ctx, logIndexProgress)
}

func logIndexProgress(event *cache.IndexProgressEvent) {
	if event == nil {
		return
	}
	if event.Done {
		fmt.Printf("[INFO] aerospike cache index read done%s column=%s rows=%d elapsed=%s\n", formatIndexProgressEvent(event), event.Column, event.Rows, event.Elapsed)
		return
	}
	fmt.Printf("[INFO] aerospike cache index progress%s column=%s rows=%d elapsed=%s\n", formatIndexProgressEvent(event), event.Column, event.Rows, event.Elapsed)
}

func formatIndexProgressEvent(event *cache.IndexProgressEvent) string {
	if event == nil {
		return ""
	}
	parts := make([]string, 0, 3)
	if event.View != "" {
		parts = append(parts, " view="+event.View)
	}
	if event.Dataset != "" {
		parts = append(parts, " dataset="+event.Dataset)
	}
	if event.Case != "" {
		parts = append(parts, " case="+event.Case)
	}
	return strings.Join(parts, "")
}

func warmupViewName(entry *warmupEntry) string {
	if entry == nil || entry.view == nil {
		return ""
	}
	return entry.view.Name
}

func warmupDatasetName(entry *warmupEntry) string {
	if entry == nil || entry.view == nil || entry.view.Cache == nil {
		return ""
	}
	location := strings.TrimSpace(entry.view.Name)
	if entry.view.Template != nil && entry.view.Selector != nil {
		expandedLocation, err := entry.view.Cache.ExpandedLocation(entry.view)
		if err == nil && strings.TrimSpace(expandedLocation) != "" {
			location = strings.TrimSpace(expandedLocation)
		}
	}

	namespace := warmupCacheNamespace(entry.view.Cache.Provider)
	if namespace == "" {
		return location
	}
	if location == "" {
		return namespace
	}
	return namespace + "/" + location
}

func warmupCacheNamespace(provider string) string {
	provider = strings.TrimSpace(provider)
	if provider == "" {
		return ""
	}
	scheme := url.Scheme(provider, "")
	_, namespace := url.Split(provider, scheme)
	return strings.TrimSpace(namespace)
}
