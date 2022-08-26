package reader

import (
	"context"
	"database/sql"
	"fmt"
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

	var parentMetaParam *view.MetaParam
	if session.Parent != nil {
		parentMetaParam = view.AsViewParam(session.Parent, session.Selectors.Lookup(session.Parent))
	}

	s.readAll(ctx, session, collector, &wg, errors, parentMetaParam)
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

func (s *Service) readAll(ctx context.Context, session *Session, collector *view.Collector, wg *sync.WaitGroup, errorCollector *shared.Errors, parentMeta *view.MetaParam) {
	start := time.Now()
	onFinish := session.View.Counter.Begin(start)
	defer s.afterRead(session, collector, &start, errorCollector.Error(), onFinish)

	var collectorFetchEmitted bool
	defer s.afterReadAll(collectorFetchEmitted, collector)

	aView := collector.View()
	selector := session.Selectors.Lookup(aView)
	collectorChildren := collector.Relations(selector)
	wg.Add(len(collectorChildren))

	relationGroup := sync.WaitGroup{}
	if !collector.SupportsParallel() {
		relationGroup.Add(len(collectorChildren))
	}

	for i := range collectorChildren {
		go func(i int) {
			defer s.afterRelationCompleted(wg, collector, &relationGroup)
			parentMeta := view.AsViewParam(aView, selector)
			s.readAll(ctx, session, collectorChildren[i], wg, errorCollector, parentMeta)
		}(i)
	}

	collector.WaitIfNeeded()
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

func (s *Service) exhaustRead(ctx context.Context, view *view.View, selector *view.Selector, batchData *view.BatchData, db *sql.DB, collector *view.Collector, session *Session, parentViewMetaParam *view.MetaParam) error {
	batchDataCopy := s.copyBatchData(batchData)
	batchDataCopy.ValuesBatch = batchDataCopy.Values

	wg := &sync.WaitGroup{}
	wg.Add(2)

	var queryErr error
	go func() {
		defer wg.Done()
		batchData.ValuesBatch, batchData.Parent = sliceWithLimit(batchData.Values, batchData.Parent, batchData.Parent+view.Batch.Parent)
		visitor := collector.Visitor(ctx)

		for {
			fullMatch, smartMatch, err := s.getMatchers(view, selector, batchData, collector, session)
			if err != nil {
				queryErr = err
				return
			}

			err = s.query(ctx, view, db, collector, visitor, fullMatch, smartMatch)
			if err != nil {
				queryErr = err
				return
			}

			if batchData.Parent == batchData.ParentReadSize {
				break
			}

			var nextParents int
			batchData.ValuesBatch, nextParents = sliceWithLimit(batchData.Values, batchData.Parent, batchData.Parent+view.Batch.Parent)
			batchData.Parent += nextParents
		}
	}()

	var pageErr error
	go func() {
		defer wg.Done()
		if view.Template.Meta == nil {
			return
		}

		pageErr = s.readPage(ctx, view, selector, batchDataCopy, collector, parentViewMetaParam)
	}()

	wg.Wait()
	if queryErr != nil {
		return queryErr
	}

	if pageErr != nil {
		return pageErr
	}

	return nil
}

func (s *Service) readPage(ctx context.Context, aView *view.View, selector *view.Selector, batchDataCopy *view.BatchData, collector *view.Collector, parentViewMetaParam *view.MetaParam) error {
	selectorCopy := *selector
	selector.Fields = []string{}
	selector.Columns = []string{}

	selector = &selectorCopy
	matcher, err := s.sqlBuilder.Build(aView, selector, batchDataCopy, collector.Relation(), &Exclude{Pagination: true}, parentViewMetaParam)
	if err != nil {
		return err
	}

	viewParam := view.AsViewParam(aView, selector)
	viewParam.NonWindowSQL = matcher.SQL
	viewParam.Args = matcher.Args

	SQL, args, err := aView.Template.Meta.Evaluate(selector.Parameters.Values, selector.Parameters.Has, viewParam)
	if len(args) == 0 {
		args = matcher.Args
	}

	if err != nil {
		return err
	}

	db, err := aView.Db()
	if err != nil {
		return err
	}

	slice := reflect.New(aView.Template.Meta.Schema.SliceType())
	slicePtr := unsafe.Pointer(slice.Pointer())
	appender := aView.Template.Meta.Schema.Slice().Appender(slicePtr)
	reader, err := read.New(ctx, db, SQL, func() interface{} {
		add := appender.Add()
		return add
	})

	if err != nil {
		return err
	}

	err = reader.QueryAll(ctx, func(row interface{}) error {
		return collector.AddMeta(row)
	}, args...)

	if err != nil {
		return err
	}

	return nil
}

func (s *Service) copyBatchData(batchData *view.BatchData) *view.BatchData {
	batchDataCopy := *batchData
	return &batchDataCopy
}

func (s *Service) getMatchers(aView *view.View, selector *view.Selector, batchData *view.BatchData, collector *view.Collector, session *Session) (fullMatch *cache.Matcher, columnInMatcher *cache.Matcher, err error) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	var fullMatchErr, smartMatchErr error
	go func() {
		defer wg.Done()

		data, _ := session.ParentData()
		fullMatch, fullMatchErr = s.sqlBuilder.Build(aView, selector, batchData, collector.Relation(), nil, data.AsParam())
	}()

	go func() {
		defer wg.Done()

		if aView.Cache != nil && aView.Cache.Warmup != nil {

			data, _ := session.ParentData()
			columnInMatcher, smartMatchErr = s.sqlBuilder.Build(aView, selector, batchData, collector.Relation(), &Exclude{Pagination: true, ColumnsIn: true}, data.AsParam())
		}
	}()

	wg.Wait()
	return fullMatch, columnInMatcher, notNilErr(fullMatchErr, smartMatchErr)
}

func notNilErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) query(ctx context.Context, aView *view.View, db *sql.DB, collector *view.Collector, visitor view.Visitor, fullMatcher, columnInMatcher *cache.Matcher) error {
	begin := time.Now()

	var options = []option.Option{io.Resolve(collector.Resolve)}
	if aView.Cache != nil {
		service, err := aView.Cache.Service()
		if err == nil {
			options = append(options, service)
		}
	}

	if columnInMatcher != nil {
		options = append(options, &columnInMatcher)
	}

	reader, err := read.New(ctx, db, fullMatcher.SQL, collector.NewItem(), options...)
	if err != nil {
		aView.Logger.LogDatabaseErr(fullMatcher.SQL, err)
		return fmt.Errorf("database error occured while fetching data for view %v", aView.Name)
	}

	defer func() {
		stmt := reader.Stmt()
		if stmt == nil {
			return
		}
		stmt.Close()
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
		return fmt.Errorf("database error occured while fetching data for view %v", aView.Name)
	}

	return nil
}

// New creates Service instance
func New() *Service {
	return &Service{
		sqlBuilder: NewBuilder(),
		Resource:   view.EmptyResource(),
	}
}
