package reader

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/transform/expand"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric/counter"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/option"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

// Service represents reader service
type Service struct {
	sqlBuilder *Builder
	Resource   *view.Resource
}

// Read select view from database based on View and assign it to dest. ParentDest has to be pointer.
func (s *Service) Read(ctx context.Context, session *Session) error {
	var err error
	if err = session.Init(); err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	collector := session.View.Collector(session.Dest, session.HandleViewMeta, session.View.MatchStrategy.SupportsParallel())
	errors := shared.NewErrors(0)

	s.readAll(ctx, session, collector, &wg, errors, session.Parent)
	wg.Wait()
	err = errors.Error()
	if err != nil {
		return err
	}
	collector.MergeData()

	if err = errors.Error(); err != nil {
		return err
	}

	if dest, ok := session.Dest.(*interface{}); ok {
		*dest = collector.Dest()
	}
	return nil
}

func (s *Service) afterRead(session *Session, collector *view.Collector, start *time.Time, err error, onFinish counter.OnDone) {
	end := time.Now()
	viewName := collector.View().Name
	session.View.Logger.ReadTime(viewName, start, &end, err)
	//TODO add to metrics record read
	elapsed := end.Sub(*start)

	session.AddMetric(&Metric{View: viewName, ElapsedMs: int(elapsed.Milliseconds()), Elapsed: elapsed.String(), Rows: collector.Len()})
	if err != nil {
		session.View.Counter.IncrementValue(Error)
	} else {
		session.View.Counter.IncrementValue(Success)
	}
	onFinish(end)
}

func (s *Service) readAll(ctx context.Context, session *Session, collector *view.Collector, wg *sync.WaitGroup, errorCollector *shared.Errors, parent *view.View) {
	if errorCollector.Error() != nil {
		return
	}

	start := time.Now()
	onFinish := session.View.Counter.Begin(start)
	defer s.afterRead(session, collector, &start, errorCollector.Error(), onFinish)

	var collectorFetchEmitted bool
	defer s.afterReadAll(collectorFetchEmitted, collector)

	aView := collector.View()
	selector := session.Selectors.Lookup(aView)
	collectorChildren, err := collector.Relations(selector)
	if err != nil {
		errorCollector.Append(err)
		return
	}

	wg.Add(len(collectorChildren))
	relationGroup := sync.WaitGroup{}
	if !collector.SupportsParallel() {
		relationGroup.Add(len(collectorChildren))
	}

	for i := range collectorChildren {
		go func(i int, parent *view.View) {
			defer s.afterRelationCompleted(wg, collector, &relationGroup)
			s.readAll(ctx, session, collectorChildren[i], wg, errorCollector, aView)
		}(i, aView)
	}

	collector.WaitIfNeeded()
	if err := errorCollector.Error(); err != nil {
		return
	}

	batchData := s.batchData(collector)
	if batchData.ColumnName != "" && len(batchData.Values) == 0 {
		return
	}

	db, err := aView.Db()
	if err != nil {
		errorCollector.Append(err)
		return
	}

	session.View.Counter.IncrementValue(Pending)
	defer session.View.Counter.DecrementValue(Pending)

	parentMeta := view.AsViewParam(aView, selector, batchData)
	err = s.exhaustRead(ctx, aView, selector, batchData, db, collector, session, parentMeta)
	if err != nil {
		errorCollector.Append(err)
	}

	if collector.SupportsParallel() {
		return
	}

	collectorFetchEmitted = true
	collector.Fetched()

	relationGroup.Wait()
	ptr, xslice := collector.Slice()
	for i := 0; i < xslice.Len(ptr); i++ {
		if actual, ok := xslice.ValuePointerAt(ptr, i).(OnRelationer); ok {
			actual.OnRelation(ctx)
			continue
		}

		break
	}
}

func (s *Service) afterRelationCompleted(wg *sync.WaitGroup, collector *view.Collector, relationGroup *sync.WaitGroup) {
	wg.Done()
	if collector.SupportsParallel() {
		return
	}
	relationGroup.Done()
}

func (s *Service) afterReadAll(collectorFetchEmitted bool, collector *view.Collector) {
	if collectorFetchEmitted {
		return
	}
	collector.Fetched()
}

func (s *Service) batchData(collector *view.Collector) *view.BatchData {
	batchData := &view.BatchData{}

	batchData.Values, batchData.ColumnName = collector.ParentPlaceholders()
	batchData.ParentReadSize = len(batchData.Values)

	return batchData
}

func (s *Service) exhaustRead(ctx context.Context, view *view.View, selector *view.Selector, batchData *view.BatchData, db *sql.DB, collector *view.Collector, session *Session, parentViewMetaParam *expand.MetaParam) error {
	batchDataCopy := s.copyBatchData(batchData)
	wg := &sync.WaitGroup{}

	info := &Info{
		View: view.Name,
	}

	var metaErr error
	if view.Template.Meta != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info.TemplateMeta, metaErr = s.readMeta(ctx, session, view, selector, batchDataCopy, collector, parentViewMetaParam)
		}()
	}

	var err error
	if info.Template, err = s.readObjects(ctx, session, batchData, view, collector, selector, db, info); err != nil {
		return err
	}

	wg.Wait()

	session.AddInfo(info)
	return metaErr
}

func (s *Service) readObjects(ctx context.Context, session *Session, batchData *view.BatchData, view *view.View, collector *view.Collector, selector *view.Selector, db *sql.DB, info *Info) ([]*Stats, error) {
	batchData.ValuesBatch, batchData.Parent = sliceWithLimit(batchData.Values, batchData.Parent, batchData.Parent+view.Batch.Parent)
	visitor := collector.Visitor(ctx)
	var stats []*Stats

	for {
		fullMatch, smartMatch, err := s.getMatchers(view, selector, batchData, collector, session)
		if err != nil {
			return stats, err
		}

		currStats, err := s.query(ctx, session, view, db, collector, visitor, fullMatch, smartMatch)
		if err != nil {
			return stats, err
		}

		if currStats != nil {
			stats = append(stats, currStats)
		}

		if batchData.Parent == batchData.ParentReadSize {
			break
		}

		var nextParents int
		batchData.ValuesBatch, nextParents = sliceWithLimit(batchData.Values, batchData.Parent, batchData.Parent+view.Batch.Parent)
		batchData.Parent += nextParents
	}

	return stats, nil
}

func (s *Service) readMeta(ctx context.Context, session *Session, aView *view.View, selector *view.Selector, batchDataCopy *view.BatchData, collector *view.Collector, parentViewMetaParam *expand.MetaParam) ([]*Stats, error) {
	selectorCopy := *selector
	selector.Fields = []string{}
	selector.Columns = []string{}
	selector = &selectorCopy

	var indexed *cache.Index
	var cacheStats *cache.Stats
	var metaOptions []option.Option
	wg := &sync.WaitGroup{}
	wg.Add(1)

	var cacheErr error
	go func() {
		defer wg.Done()
		if !session.IsCacheEnabled(aView) {
			return
		}

		cacheMatcher, err := s.sqlBuilder.CacheMetaSQL(aView, selector, batchDataCopy, collector.Relation(), parentViewMetaParam)
		if err != nil {
			cacheErr = err
			return
		}

		cacheService, err := aView.Cache.Service()
		if err != nil {
			return
		}

		cacheStats = &cache.Stats{}
		metaOptions = []option.Option{cacheService, cacheMatcher, cacheStats}
	}()

	var err error
	indexed, err = s.sqlBuilder.ExactMetaSQL(aView, selector, batchDataCopy, collector.Relation(), parentViewMetaParam)
	if err != nil {
		return nil, err
	}

	wg.Wait()
	if cacheErr != nil {
		return nil, cacheErr
	}

	db, err := aView.Db()
	if err != nil {
		return nil, err
	}

	slice := reflect.New(aView.Template.Meta.Schema.SliceType())
	slicePtr := unsafe.Pointer(slice.Pointer())
	appender := aView.Template.Meta.Schema.Slice().Appender(slicePtr)
	reader, err := read.New(ctx, db, indexed.SQL, func() interface{} {
		add := appender.Add()
		return add
	}, metaOptions...)

	if err != nil {
		return nil, err
	}

	defer func() {
		stmt := reader.Stmt()
		if stmt == nil {
			return
		}

		_ = stmt.Close()
	}()

	err = reader.QueryAll(ctx, func(row interface{}) error {
		return collector.AddMeta(row)
	}, indexed.Args...)

	if err != nil {
		return nil, err
	}

	return []*Stats{
		s.NewStats(session, indexed, cacheStats),
	}, nil
}

func (s *Service) copyBatchData(batchData *view.BatchData) *view.BatchData {
	batchDataCopy := *batchData
	newValues := make([]interface{}, len(batchData.Values))
	copy(newValues, batchDataCopy.Values)
	batchDataCopy.ValuesBatch = newValues

	return &batchDataCopy
}

func (s *Service) getMatchers(aView *view.View, selector *view.Selector, batchData *view.BatchData, collector *view.Collector, session *Session) (fullMatch *cache.Index, columnInMatcher *cache.Index, err error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	relation := collector.Relation()

	var cacheErr error
	go func() {
		defer wg.Done()

		if (aView.Cache != nil && aView.Cache.Warmup != nil) || relation != nil {
			data, _ := session.ParentData()
			columnInMatcher, cacheErr = s.sqlBuilder.CacheSQLWithOptions(aView, selector, batchData, relation, data.AsParam())
		}
	}()

	data, _ := session.ParentData()
	fullMatch, err = s.sqlBuilder.Build(aView, selector, batchData, relation, &Exclude{
		Pagination: relation != nil && len(batchData.ValuesBatch) > 1,
	}, data.AsParam(), nil)

	if err != nil {
		return nil, nil, err
	}

	wg.Wait()
	return fullMatch, columnInMatcher, cacheErr
}

func (s *Service) query(ctx context.Context, session *Session, aView *view.View, db *sql.DB, collector *view.Collector, visitor view.Visitor, fullMatcher, columnInMatcher *cache.Index) (*Stats, error) {
	begin := time.Now()

	var cacheStats *cache.Stats
	var options = []option.Option{io.Resolve(collector.Resolve)}
	if session.IsCacheEnabled(aView) {
		service, err := aView.Cache.Service()
		if err != nil {
			fmt.Printf("err: %v\n", err.Error())
		}

		if err == nil {
			cacheStats = &cache.Stats{}
			options = append(options, service, cacheStats)
		}
	}

	if columnInMatcher != nil {
		columnInMatcher.OnSkip = collector.OnSkip
		options = append(options, &columnInMatcher)
	}

	stats := s.NewStats(session, fullMatcher, cacheStats)

	reader, err := read.New(ctx, db, fullMatcher.SQL, collector.NewItem(), options...)
	if err != nil {
		if session.IncludeSQL {
			return nil, err
		}

		aView.Logger.LogDatabaseErr(fullMatcher.SQL, err)
		stats.Error = err.Error()
		return nil, fmt.Errorf("database error occured while fetching data for view %v", aView.Name)
	}

	defer func() {
		stmt := reader.Stmt()
		if stmt == nil {
			return
		}

		_ = stmt.Close()
	}()

	readData := 0
	err = reader.QueryAll(ctx, func(row interface{}) error {
		row, err = aView.UnwrapDatabaseType(ctx, row)
		if err != nil {
			return err
		}

		readData++
		if fetcher, ok := row.(OnFetcher); ok {
			if err = fetcher.OnFetch(ctx); err != nil {
				return err
			}
		}
		return visitor(row)
	}, fullMatcher.Args...)
	end := time.Now()
	aView.Logger.ReadingData(end.Sub(begin), fullMatcher.SQL, readData, fullMatcher.Args, err)
	if err != nil {
		aView.Logger.LogDatabaseErr(fullMatcher.SQL, err)
		return nil, fmt.Errorf("database error occured while fetching data for view %v", aView.Name)
	}

	return stats, nil
}

func (s *Service) NewStats(session *Session, index *cache.Index, cacheStats *cache.Stats) *Stats {
	var SQL string
	var args []interface{}
	if session.IncludeSQL {
		SQL = index.SQL
		args = index.Args
	}

	return &Stats{
		SQL:        SQL,
		Args:       args,
		CacheStats: cacheStats,
	}
}

// New creates Service instance
func New() *Service {
	return &Service{
		sqlBuilder: NewBuilder(),
		Resource:   view.EmptyResource(),
	}
}
