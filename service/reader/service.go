package reader

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/shared"
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
}

// ReadInto reads Data into provided destination, * dDest` is required. It has to be a pointer to `interface{}` or pointer to slice of `T` or `*T`
func (s *Service) ReadInto(ctx context.Context, dest interface{}, aView *view.View, opts ...Option) error {
	session, err := NewSession(dest, aView, opts...)
	if err != nil {
		return err
	}
	return s.Read(ctx, session)
}

// Read select view from database based on View and assign it to dest. ParentDest has to be pointer.
func (s *Service) Read(ctx context.Context, session *Session) error {
	err := s.read(ctx, session)
	return err
}

func (s *Service) read(ctx context.Context, session *Session) error {
	var err error
	if err = session.Init(); err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	collector := session.View.Collector(session.DataPtr, session.HandleViewMeta, session.View.MatchStrategy.SupportsParallel())
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

	if dest, ok := session.DataPtr.(*interface{}); ok {
		*dest = collector.Dest()
	}
	session.syncData(session.View.Schema.Cardinality)
	state := session.State.Lookup(session.View)
	session.Filters = state.Filters
	return nil
}

func (s *Service) afterRead(session *Session, collector *view.Collector, start *time.Time, info *TemplateExecution, err error, onFinish counter.OnDone) {
	end := Now()
	viewName := collector.View().Name
	session.View.Logger.ReadTime(viewName, start, &end, err)
	elapsed := Dif(end, *start)
	session.AddMetric(&Metric{View: viewName, Execution: info, ElapsedMs: int(elapsed.Milliseconds()), Elapsed: elapsed.String(), Rows: collector.Len()})
	if err != nil {
		session.View.Counter.IncrementValue(Error)
	} else {
		session.View.Counter.IncrementValue(Success)
	}
	onFinish(end)
	info.Elapsed = elapsed.String()
}

func (s *Service) readAll(ctx context.Context, session *Session, collector *view.Collector, wg *sync.WaitGroup, errorCollector *shared.Errors, parent *view.View) {
	if errorCollector.Error() != nil {
		return
	}

	var collectorFetchEmitted bool
	defer s.afterReadAll(collectorFetchEmitted, collector)

	aView := collector.View()
	selector := session.State.Lookup(aView)
	if selector.Ignore {
		return
	}

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
	if len(batchData.ColumnNames) != 0 && len(batchData.Values) == 0 {
		return
	}

	session.View.Counter.IncrementValue(Pending)
	defer session.View.Counter.DecrementValue(Pending)

	err = s.exhaustRead(ctx, aView, selector, batchData, collector, session)
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

	batchData.Values, batchData.ColumnNames = collector.ParentPlaceholders()
	batchData.ParentReadSize = len(batchData.Values)

	return batchData
}

func (s *Service) exhaustRead(ctx context.Context, view *view.View, selector *view.Statelet, batchData *view.BatchData, collector *view.Collector, session *Session) error {
	execution := &TemplateExecution{}
	start := Now()
	onFinish := session.View.Counter.Begin(start)
	err := s.readObjectsWithMeta(ctx, session, batchData, view, collector, selector, execution)
	s.afterRead(session, collector, &start, execution, err, onFinish)
	return err
}

func (s *Service) readObjectsWithMeta(ctx context.Context, session *Session, batchData *view.BatchData, view *view.View, collector *view.Collector, selector *view.Statelet, info *TemplateExecution) error {
	batchData.ValuesBatch, batchData.Parent = sliceWithLimit(batchData.Values, batchData.Parent, batchData.Parent+view.Batch.Parent)
	visitor := collector.Visitor(ctx)

	for {
		err := s.queryObjectsWithMeta(ctx, session, view, collector, visitor, info, batchData, selector)
		if err != nil {
			return err
		}

		if batchData.Parent == batchData.ParentReadSize {
			break
		}

		var nextParents int
		batchData.ValuesBatch, nextParents = sliceWithLimit(batchData.Values, batchData.Parent, batchData.Parent+view.Batch.Parent)
		batchData.Parent += nextParents
	}

	return nil
}

func (s *Service) queryMeta(ctx context.Context, session *Session, aView *view.View, originalSelector *view.Statelet, batchDataCopy *view.BatchData, collector *view.Collector, parentViewMetaParam *expand.MetaParam) (*SQLExecution, error) {
	selectorDeref := *originalSelector
	selectorDeref.Fields = []string{}
	selectorDeref.Columns = []string{}
	selector := &selectorDeref

	var indexed *cache.ParmetrizedQuery
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

	slice := reflect.New(aView.Template.Summary.Schema.SliceType())
	slicePtr := unsafe.Pointer(slice.Pointer())
	appender := aView.Template.Summary.Schema.Slice().Appender(slicePtr)

	SQL := indexed.SQL
	args := indexed.Args
	now := Now()

	reader, err := read.New(ctx, db, SQL, func() interface{} {
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
	}, args...)

	if err != nil {
		return nil, err
	}

	finished := Now()
	aView.Logger.Log("reading view %v meta took %v, SQL: %v , Args: %v\n", aView.Name, finished.Sub(now).String(), SQL, args)

	return s.NewExecutionInfo(session, indexed, cacheStats, cacheErr), nil
}

func (s *Service) getMatchers(aView *view.View, selector *view.Statelet, batchData *view.BatchData, collector *view.Collector, session *Session) (fullMatch *cache.ParmetrizedQuery, columnInMatcher *cache.ParmetrizedQuery, err error) {
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

func (s *Service) queryObjectsWithMeta(ctx context.Context, session *Session, aView *view.View, collector *view.Collector, visitor view.VisitorFn, info *TemplateExecution, batchData *view.BatchData, selector *view.Statelet) error {
	wg := &sync.WaitGroup{}
	db, err := aView.Db()
	if err != nil {
		return err
	}

	var metaErr error
	if aView.Template.Summary != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var templateMeta *SQLExecution
			parentMeta := view.AsViewParam(aView, selector, batchData)
			templateMeta, metaErr = s.queryMeta(ctx, session, aView, selector, batchData, collector, parentMeta)
			if templateMeta != nil {
				info.TemplateMeta = append(info.TemplateMeta, templateMeta)
			}
		}()
	}

	objectStats, err := s.queryObjects(ctx, session, aView, selector, batchData, db, collector, visitor)
	if err != nil {
		return err
	}

	if objectStats != nil {
		info.Template = append(info.Template, objectStats)
	}
	wg.Wait()
	return metaErr
}

func (s *Service) queryObjects(ctx context.Context, session *Session, aView *view.View, selector *view.Statelet, batchData *view.BatchData, db *sql.DB, collector *view.Collector, visitor view.VisitorFn) (*SQLExecution, error) {
	fullMatcher, columnInMatcher, err := s.getMatchers(aView, selector, batchData, collector, session)
	if err != nil {
		return nil, err
	}

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
	if session.CacheRefresh {
		options = append(options, session.CacheRefresh)
	}

	stats := s.NewExecutionInfo(session, fullMatcher, cacheStats, err)
	if session.DryRun {
		return stats, nil
	}

	reader, err := read.New(ctx, db, fullMatcher.SQL, collector.NewItem(), options...)
	if err != nil {
		return s.HandleSQLError(err, session, aView, fullMatcher, stats)
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
		return s.HandleSQLError(err, session, aView, fullMatcher, stats)
	}

	return stats, nil
}

func (s *Service) HandleSQLError(err error, session *Session, aView *view.View, matcher *cache.ParmetrizedQuery, stats *SQLExecution) (*SQLExecution, error) {
	if session.IncludeSQL {
		return nil, err
	}

	aView.Logger.LogDatabaseErr(matcher.SQL, err, matcher.Args...)
	stats.Error = err.Error()
	return nil, fmt.Errorf("database error occured while fetching Data for view %v", aView.Name)
}

func (s *Service) NewExecutionInfo(session *Session, index *cache.ParmetrizedQuery, cacheStats *cache.Stats, cacheError error) *SQLExecution {
	var SQL string
	var args []interface{}
	if session.IncludeSQL {
		SQL = index.SQL
		args = index.Args
	}

	var cacheErrorMessage string
	if cacheError != nil {
		cacheErrorMessage = cacheError.Error()
	}

	return &SQLExecution{
		SQL:        SQL,
		Args:       args,
		CacheStats: cacheStats,
		CacheError: cacheErrorMessage,
	}
}

// New creates Service instance
func New() *Service {
	ret := &Service{
		sqlBuilder: NewBuilder(),
	}
	return ret
}
