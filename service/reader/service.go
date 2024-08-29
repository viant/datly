package reader

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/gmetric/counter"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/option"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler/response"
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
	err = s.Read(ctx, session)
	if session.MetricPtr != nil {
		*session.MetricPtr = session.Metrics
	}
	return err
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
	collector := session.View.Collector(session.DataPtr, session.HandleViewMeta, session.View.MatchStrategy.ReadAll())
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

func (s *Service) afterRead(session *Session, collector *view.Collector, start *time.Time, info *response.Execution, err error, onFinish counter.OnDone) {
	end := Now()
	viewName := collector.View().Name
	session.View.Logger.ReadTime(viewName, start, &end, err)
	elapsed := Dif(end, *start)
	session.AddMetric(&response.Metric{View: viewName, Execution: info, ElapsedMs: int(elapsed.Milliseconds()), Elapsed: elapsed.String(), Rows: collector.Len()})
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
	if !collector.ReadAll() {
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

	if collector.ReadAll() {
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
	if collector.ReadAll() {
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

func nopCounterDone(end time.Time, values ...interface{}) int64 {
	return 0
}

func (s *Service) exhaustRead(ctx context.Context, view *view.View, selector *view.Statelet, batchData *view.BatchData, collector *view.Collector, session *Session) error {
	execution := &response.Execution{}
	start := Now()
	onFinish := nopCounterDone
	if !session.DryRun {
		onFinish = view.Counter.Begin(start)
	}
	err := s.readObjects(ctx, session, batchData, view, collector, selector, execution)
	s.afterRead(session, collector, &start, execution, err, onFinish)
	return err
}

func (s *Service) readObjects(ctx context.Context, session *Session, batchData *view.BatchData, view *view.View, collector *view.Collector, selector *view.Statelet, info *response.Execution) error {

	/*
		TODO paralelize batching if relation is to one
		isToOne := false
		if rel := collector.Relation(); rel != nil {

			fmt.Printf("PARENT: %v\n", collector.Relation().Cardinality)
		}
	*/

	batchData.ValuesBatch, batchData.Size = sliceWithLimit(batchData.Values, batchData.Size, batchData.Size+view.Batch.Size)
	visitor := collector.Visitor(ctx)
	for {
		err := s.queryInBatches(ctx, session, view, collector, visitor, info, batchData, selector)
		if err != nil {
			return err
		}
		if batchData.Size >= batchData.ParentReadSize {
			break
		}
		var nextParents int
		batchData.ValuesBatch, nextParents = sliceWithLimit(batchData.Values, batchData.Size, batchData.Size+view.Batch.Size)
		batchData.Size += nextParents
	}
	return nil
}

func (s *Service) querySummary(ctx context.Context, session *Session, aView *view.View, statelet *view.Statelet, batchDataCopy *view.BatchData, collector *view.Collector, parentViewMetaParam *expand.ViewContext) (*response.SQLExecution, error) {
	selectorDeref := *statelet
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

		cacheMatcher, err := s.sqlBuilder.CacheMetaSQL(ctx, aView, selector, batchDataCopy, collector.Relation(), parentViewMetaParam)
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
	indexed, err = s.sqlBuilder.ExactMetaSQL(ctx, aView, selector, batchDataCopy, collector.Relation(), parentViewMetaParam)
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

func (s *Service) buildParametrizedSQL(ctx context.Context, aView *view.View, statelet *view.Statelet, batchData *view.BatchData, collector *view.Collector, session *Session, partitions *view.Partitions) (parametrizedSQL *cache.ParmetrizedQuery, columnInMatcher *cache.ParmetrizedQuery, err error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	relation := collector.Relation()

	var cacheErr error
	go func() {
		defer wg.Done()
		if (aView.Cache != nil && aView.Cache.Warmup != nil) || relation != nil {
			data, _ := session.ParentData()
			columnInMatcher, cacheErr = s.sqlBuilder.CacheSQLWithOptions(ctx, aView, statelet, batchData, relation, data.AsParam())
		}
	}()

	data, _ := session.ParentData()
	parametrizedSQL, err = s.sqlBuilder.Build(ctx, WithBuilderView(aView), WithBuilderStatelet(statelet), WithBuilderPartitions(partitions), WithBuilderBatchData(batchData), WithBuilderRelation(relation), WithBuilderExclude(
		false, relation != nil && len(batchData.ValuesBatch) > 1), WithBuilderParent(data.AsParam()))
	if err != nil {
		return nil, nil, err
	}
	wg.Wait()
	return parametrizedSQL, columnInMatcher, cacheErr
}

func (s *Service) BuildCriteria(ctx context.Context, value interface{}, options *codec.CriteriaBuilderOptions) (*codec.Criteria, error) {
	baseView := view.Context(ctx)
	aSchema := state.NewSchema(reflect.TypeOf(value))
	resource := baseView.Resource()
	aType, err := state.NewType(state.WithSchema(aSchema), state.WithResource(resource))
	if err != nil {
		return nil, err
	}
	if err := aType.Init(state.WithResource(resource)); err != nil {
		return nil, err
	}
	aView, err := view.New("autogen", "", view.WithTemplate(view.NewTemplate(options.Expression, view.WithTemplateSchema(aSchema), view.WithTemplateParameters(aType.Parameters...))))
	if err != nil {
		return nil, err
	}
	err = aView.Template.Init(ctx, baseView.GetResource(), aView)
	if err != nil {
		return nil, err
	}
	stateType := structology.NewStateType(reflect.TypeOf(value))
	statelet := &view.Statelet{Template: stateType.NewState()}
	parametrizedSQL, err := s.sqlBuilder.Build(ctx, WithBuilderView(aView), WithBuilderStatelet(statelet))
	if err != nil {
		return nil, err
	}
	ret := &codec.Criteria{Expression: parametrizedSQL.SQL, Placeholders: parametrizedSQL.Args}
	return ret, nil
}

func (s *Service) queryInBatches(ctx context.Context, session *Session, aView *view.View, collector *view.Collector, visitor view.VisitorFn, info *response.Execution, batchData *view.BatchData, selector *view.Statelet) error {
	wg := &sync.WaitGroup{}
	db, err := aView.Db()
	if err != nil {
		return fmt.Errorf("failed to get db: %w", err)
	}

	var metaErr error
	if aView.Template.Summary != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var templateMeta *response.SQLExecution
			parentMeta := view.AsViewParam(aView, selector, batchData)
			templateMeta, metaErr = s.querySummary(ctx, session, aView, selector, batchData, collector, parentMeta)
			if templateMeta != nil {
				info.Summary = append(info.Summary, templateMeta)
			}
		}()
	}

	executions, err := s.queryObjects(ctx, session, aView, selector, batchData, db, collector, visitor)
	if err != nil {
		return err
	}

	if len(executions) > 0 {
		info.View = append(info.View, executions...)
	}
	wg.Wait()
	return metaErr
}

func (s *Service) queryObjects(ctx context.Context, session *Session, aView *view.View, selector *view.Statelet, batchData *view.BatchData, db *sql.DB, collector *view.Collector, visitor view.VisitorFn) ([]*response.SQLExecution, error) {

	if partitioned := aView.Partitioned; partitioned != nil {
		return s.queryWithPartitions(ctx, session, aView, selector, batchData, db, collector, visitor, partitioned)
	}
	readData := 0
	parametrizedSQL, columnInMatcher, err := s.buildParametrizedSQL(ctx, aView, selector, batchData, collector, session, nil)
	if err != nil {
		return nil, err
	}

	handler := func(row interface{}) error {
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
	}
	return s.queryWithHandler(ctx, session, aView, collector, columnInMatcher, parametrizedSQL, db, handler, &readData)
}

func (s *Service) queryWithHandler(ctx context.Context, session *Session, aView *view.View, collector *view.Collector, columnInMatcher *cache.ParmetrizedQuery, parametrizedSQL *cache.ParmetrizedQuery, db *sql.DB, handler func(row interface{}) error, readData *int) ([]*response.SQLExecution, error) {
	begin := time.Now()
	var cacheStats *cache.Stats
	var options = []option.Option{io.Resolve(collector.Resolve)}
	var err error
	if session.IsCacheEnabled(aView) {
		service, err := aView.Cache.Service()
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

	stats := s.NewExecutionInfo(session, parametrizedSQL, cacheStats, err)
	if session.DryRun {
		return []*response.SQLExecution{stats}, nil
	}

	reader, err := read.New(ctx, db, parametrizedSQL.SQL, collector.NewItem(), options...)
	if err != nil {
		exec, err := s.HandleSQLError(err, session, aView, parametrizedSQL, stats)
		return []*response.SQLExecution{exec}, err
	}
	defer func() {
		stmt := reader.Stmt()
		if stmt == nil {
			return
		}
		_ = stmt.Close()
	}()

	err = reader.QueryAll(ctx, handler, parametrizedSQL.Args...)
	end := time.Now()
	aView.Logger.ReadingData(end.Sub(begin), parametrizedSQL.SQL, *readData, parametrizedSQL.Args, err)
	if err != nil {
		exec, err := s.HandleSQLError(err, session, aView, parametrizedSQL, stats)
		return []*response.SQLExecution{exec}, err
	}
	return []*response.SQLExecution{stats}, nil
}

func (s *Service) queryWithPartitions(ctx context.Context, session *Session, aView *view.View, selector *view.Statelet, batchData *view.BatchData, db *sql.DB, collector *view.Collector, visitor view.VisitorFn, partitioned *view.Partitioned) ([]*response.SQLExecution, error) {

	concurrency := aView.Partitioned.Concurrency
	if concurrency == 0 {
		concurrency = 2
	}
	partitioner := partitioned.Partitioner()
	wg := &sync.WaitGroup{}
	partitions := partitioner.Partitions(ctx, db, aView)
	repeat := max(1, len(partitions.Partitions))
	var executions []*response.SQLExecution
	var mux sync.Mutex
	var err error

	var rateLimit = make(chan bool, concurrency)
	var collectors = make([]*view.Collector, repeat)
	for i := 0; i < repeat; i++ {
		clone := *partitions
		wg.Add(1)
		rateLimit <- true
		go func(index int, partitions *view.Partitions) {
			defer func() {
				wg.Done()
				<-rateLimit
			}()
			if index < len(partitions.Partitions) {
				partitions.Partition = partitions.Partitions[index]
			}

			collectors[index] = collector.Clone()
			parametrizedSQL, columnInMatcher, e := s.buildParametrizedSQL(ctx, aView, selector, batchData, collectors[index], session, partitions)
			readData := 0
			handler := func(row interface{}) error {
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
				return nil
			}
			exec, e := s.queryWithHandler(ctx, session, aView, collectors[index], columnInMatcher, parametrizedSQL, db, handler, &readData)
			mux.Lock()
			if exec != nil {
				executions = append(executions, exec...)
			}
			mux.Unlock()
			if e != nil {
				err = e
			}
		}(i, &clone)
		if err != nil {
			break
		}
	}
	wg.Wait()
	if len(collectors) == 0 || err != nil {
		return executions, err
	}

	result := collectors[0]

	for i := 1; i < len(collectors); i++ {
		second := collectors[i]
		merged := combineSlices(result.Dest(), second.Dest())
		result.SetDest(merged)
	}

	if newReducer, ok := partitioner.(view.ReducerProvider); ok {
		reducer := newReducer.Reducer(ctx)
		reduced := reducer.Reduce(result.Dest())
		result.SetDest(reduced)
	}

	resultValue := reflect.ValueOf(result.Dest())
	for i := 0; i < resultValue.Len(); i++ {
		value := resultValue.Index(i).Interface()
		if err := visitor(value); err != nil {
			return executions, err
		}
	}

	collector.SetDest(result.Dest())

	return executions, err
}

func (s *Service) HandleSQLError(err error, session *Session, aView *view.View, matcher *cache.ParmetrizedQuery, stats *response.SQLExecution) (*response.SQLExecution, error) {
	if session.IncludeSQL {
		return nil, err
	}
	aView.Logger.LogDatabaseErr(matcher.SQL, err, matcher.Args...)
	stats.Error = err.Error()
	return nil, fmt.Errorf("database error occured while fetching Data for view %v %w", aView.Name, err)
}

func (s *Service) NewExecutionInfo(session *Session, index *cache.ParmetrizedQuery, cacheStats *cache.Stats, cacheError error) *response.SQLExecution {
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
	var cache *response.CacheStats
	if cacheStats != nil {
		cache = &response.CacheStats{
			Type:           string(cacheStats.Type),
			RecordsCounter: cacheStats.RecordsCounter,
			Key:            cacheStats.Key,
			Dataset:        cacheStats.Dataset,
			Namespace:      cacheStats.Namespace,
			FoundWarmup:    cacheStats.FoundWarmup,
			FoundLazy:      cacheStats.FoundLazy,
			ErrorType:      cacheStats.ErrorType,
			ErrorCode:      int(cacheStats.ErrorCode),
			ExpiryTime:     cacheStats.ExpiryTime,
		}
	}
	return &response.SQLExecution{
		SQL:        SQL,
		Args:       args,
		CacheStats: cache,
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
