package reader

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/shared"
	"github.com/viant/gmetric/counter"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"time"
)

//Service represents reader service
type Service struct {
	sqlBuilder *Builder
	Resource   *data.Resource
}

//Read select data from database based on View and assign it to dest. ParentDest has to be pointer.
//TODO: Select with join when connector is the same for one to one relation
func (s *Service) Read(ctx context.Context, session *Session) error {
	var err error
	start := time.Now()
	onFinish := session.View.Counter.Begin(start)
	defer s.afterRead(session, &start, err, onFinish)

	if err = session.Init(ctx, s.Resource); err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	collector := session.View.Collector(session.Dest, session.View.MatchStrategy.SupportsParallel())
	errors := shared.NewErrors(0)
	s.readAll(ctx, session, collector, nil, &wg, errors)
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

func (s *Service) afterRead(session *Session, start *time.Time, err error, onFinish counter.OnDone) {
	end := time.Now()
	session.View.Logger.ReadTime(session.View.Name, start, &end, err)
	if err != nil {
		session.View.Counter.IncrementValue(Error)
	} else {
		session.View.Counter.IncrementValue(Success)
	}
	onFinish(end)
}

func (s *Service) readAll(ctx context.Context, session *Session, collector *data.Collector, upstream rdata.Map, wg *sync.WaitGroup, errorCollector *shared.Errors) {
	var collectorFetchEmitted bool
	defer s.afterReadAll(collectorFetchEmitted, collector)

	view := collector.View()
	params, err := s.buildViewParams(ctx, session, view)

	if err != nil {
		errorCollector.Append(err)
		return
	}

	selector := session.Selectors.Lookup(view.Name)
	collectorChildren := collector.Relations(selector)
	wg.Add(len(collectorChildren))

	relationGroup := sync.WaitGroup{}
	if !collector.SupportsParallel() {
		relationGroup.Add(len(collectorChildren))
	}
	for i := range collectorChildren {
		go func(i int) {
			defer s.afterRelationCompleted(wg, collector, &relationGroup)
			s.readAll(ctx, session, collectorChildren[i], params, wg, errorCollector)
		}(i)
	}

	collector.WaitIfNeeded()
	batchData := s.batchData(selector, view, collector)
	if batchData.ColumnName != "" && len(batchData.Values) == 0 {
		return
	}

	db, err := view.Db()
	if err != nil {
		errorCollector.Append(err)
		return
	}

	session.View.Counter.IncrementValue(Pending)
	defer session.View.Counter.DecrementValue(Pending)
	err = s.exhaustRead(ctx, view, selector, upstream, params, batchData, db, collector)
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
		if actual, ok := xslice.ValuePointerAt(ptr, i).(OnRelationCompleter); ok {
			actual.OnRelationComplete(ctx)
			continue
		}

		break
	}
}

func (s *Service) afterRelationCompleted(wg *sync.WaitGroup, collector *data.Collector, relationGroup *sync.WaitGroup) {
	wg.Done()
	if collector.SupportsParallel() {
		return
	}
	relationGroup.Done()
}

func (s *Service) afterReadAll(collectorFetchEmitted bool, collector *data.Collector) {
	if collectorFetchEmitted {
		return
	}
	collector.Fetched()
}

func (s *Service) batchData(selector *data.Selector, view *data.View, collector *data.Collector) *BatchData {
	batchData := &BatchData{
		Read:          0,
		BatchReadSize: view.LimitWithSelector(selector),
	}

	if view.BatchReadSize != nil {
		batchData.BatchReadSize = *view.BatchReadSize
	}

	batchData.Values, batchData.ColumnName = collector.ParentPlaceholders()

	return batchData
}

func (s *Service) exhaustRead(ctx context.Context, view *data.View, selector *data.Selector, upstream rdata.Map, params rdata.Map, batchData *BatchData, db *sql.DB, collector *data.Collector) error {
	readData := 0
	limit := view.LimitWithSelector(selector)

	for {
		SQL, err := s.prepareSQL(view, selector, upstream, params, batchData, collector.Relation())
		if err != nil {
			return err
		}
		readData, err = s.query(ctx, view, db, SQL, collector, batchData)
		if err != nil {
			return err
		}

		batchData.Read = batchData.Read + readData

		if batchData.BatchReadSize == 0 || batchData.Read == limit || readData < batchData.BatchReadSize {
			break
		}

		readData = 0
	}
	return nil
}

func (s *Service) query(ctx context.Context, view *data.View, db *sql.DB, SQL string, collector *data.Collector, batchData *BatchData) (int, error) {
	begin := time.Now()
	reader, err := read.New(ctx, db, SQL, collector.NewItem(), io.Resolve(collector.Resolve))
	if err != nil {
		return 0, err
	}

	defer reader.Stmt().Close()
	visitor := collector.Visitor()
	readData := 0
	err = reader.QueryAll(ctx, func(row interface{}) error {
		readData++
		if fetcher, ok := row.(OnFetcher); ok {
			if err = fetcher.OnFetch(ctx); err != nil {
				return err
			}
		}
		return visitor(row)
	}, batchData.Values...)

	end := time.Now()
	view.Logger.ReadingData(end.Sub(begin), SQL, readData, batchData.Values, err)
	if err != nil {
		return 0, err
	}

	return readData, nil
}

func (s *Service) prepareSQL(view *data.View, selector *data.Selector, upstream rdata.Map, params rdata.Map, batchData *BatchData, relation *data.Relation) (string, error) {
	SQL, err := s.sqlBuilder.Build(view, selector, batchData, relation)
	if err != nil {
		return "", err
	}

	if len(upstream) > 0 {
		SQL = upstream.ExpandAsText(SQL)
	}

	if len(params) > 0 {
		SQL = params.ExpandAsText(SQL)
	}
	return SQL, nil
}

func (s *Service) buildViewParams(ctx context.Context, session *Session, view *data.View) (rdata.Map, error) {
	if len(view.Parameters) == 0 {
		return nil, nil
	}
	params := session.NewReplacement(view)
	for _, param := range view.Parameters {
		switch param.In.Kind {
		case data.DataViewKind:
			if err := s.addViewParams(ctx, params, param, session); err != nil {
				return nil, err
			}
		case data.PathKind:
			s.addPathParam(session, param, &params)
		case data.QueryKind:
			s.addQueryParam(session, param, &params)
		case data.HeaderKind:
			s.addHeaderParam(session, param, &params)
		case data.CookieKind:
			s.addCookieParam(session, param, &params)
		default:
			return nil, fmt.Errorf("unsupported location Kind %v", param.In.Kind)
		}
	}

	return params, nil
}

func (s *Service) addViewParams(ctx context.Context, paramMap rdata.Map, param *data.Parameter, session *Session) error {
	view := param.View()
	destSlice := reflect.New(view.Schema.SliceType()).Interface()

	collector := view.Collector(destSlice, false)
	errors := shared.NewErrors(0)

	wg := sync.WaitGroup{}
	s.readAll(ctx, session, collector, paramMap, &wg, errors)
	wg.Wait()
	if errors.Error() != nil {
		return errors.Error()
	}

	ptr := xunsafe.AsPointer(destSlice)
	paramLen := view.Schema.Slice().Len(ptr)
	switch paramLen {
	case 0:
		if param.Required != nil && *param.Required {
			return fmt.Errorf("parameter %v value is required but no data was found", param.Name)
		}
	case 1:
		holder := view.Schema.Slice().ValuePointerAt(ptr, 0)
		holderPtr := xunsafe.AsPointer(holder)
		value := view.ParamField().Interface(holderPtr)

		paramMap.SetValue(param.Name, value)
	default:
		return fmt.Errorf("parameter %v return more than one value", param.Name)
	}

	return nil
}

func (s *Service) addQueryParam(session *Session, param *data.Parameter, params *rdata.Map) {
	paramValue := session.QueryParam(param.In.Name)
	params.SetValue(param.Name, paramValue)
}

func (s *Service) addHeaderParam(session *Session, param *data.Parameter, params *rdata.Map) {
	header := session.Header(param.In.Name)
	params.SetValue(param.Name, header)
}

func (s *Service) addCookieParam(session *Session, param *data.Parameter, params *rdata.Map) {
	cookie := session.Cookie(param.In.Name)
	params.SetValue(param.Name, cookie)
}

func (s *Service) addPathParam(session *Session, param *data.Parameter, params *rdata.Map) {
	pathVariable := session.PathVariable(param.In.Name)
	params.SetValue(param.Name, pathVariable)
}

//New creates Service instance
func New() *Service {
	return &Service{
		sqlBuilder: NewBuilder(),
		Resource:   data.EmptyResource(),
	}
}
