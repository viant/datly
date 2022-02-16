package reader

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/v1/data"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

//Service represents reader service
type Service struct {
	sqlBuilder    *Builder
	AllowUnmapped AllowUnmapped
	Resource      *data.Resource
}

//TODO: batch table records
//Read select data from database based on View and assign it to dest. ParentDest has to be pointer.
//TODO: Select with join when connector is the same for one to one relation
func (s *Service) Read(ctx context.Context, session *data.Session) error {
	session.Init()

	wg := sync.WaitGroup{}

	collector := session.View.Collector(session.AllowUnmapped, session.Dest, session.View.MatchStrategy.SupportsParallel())
	err := s.readAll(ctx, session, collector, nil, &wg)
	if err != nil {
		return err
	}

	wg.Wait()
	collector.MergeData()

	if err = session.Error(); err != nil {
		return err
	}

	if dest, ok := session.Dest.(*interface{}); ok {
		*dest = collector.Dest()
	}
	return nil
}

func (s *Service) actualStructType(dest interface{}) reflect.Type {
	rType := reflect.TypeOf(dest).Elem()
	if rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType
}

func (s *Service) readAll(ctx context.Context, session *data.Session, collector *data.Collector, upstream rdata.Map, wg *sync.WaitGroup) error {
	view := collector.View()

	db, err := view.Db()
	if err != nil {
		return err
	}

	params, err := s.buildViewParams(ctx, session, view)
	if err != nil {
		return err
	}

	selector := session.Selectors.Lookup(view.Name)
	limit := view.LimitWithSelector(selector)
	batchData := s.batchData(limit, view, collector)

	if !collector.SupportsParallel() {
		err = s.readExhaust(ctx, view, selector, upstream, params, batchData, db, collector)
	} else {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.readExhaust(ctx, view, selector, upstream, params, batchData, db, collector)
			if err != nil {
				session.CollectError(err)
			}
		}()
	}

	if err = s.readRelations(ctx, session, collector, params, wg); err != nil {
		return err
	}

	return nil
}

func (s *Service) batchData(limit int, view *data.View, collector *data.Collector) *BatchData {
	batchData := &BatchData{
		CurrentlyRead: 0,
		BatchReadSize: limit,
	}

	if view.BatchReadSize != nil {
		batchData.BatchReadSize = *view.BatchReadSize
	}

	batchData.Placeholders, batchData.ColumnName = collector.ParentPlaceholders()

	return batchData
}

func (s *Service) readExhaust(ctx context.Context, view *data.View, selector *data.Selector, upstream rdata.Map, params rdata.Map, batchData *BatchData, db *sql.DB, collector *data.Collector) error {
	readData := 0
	limit := view.LimitWithSelector(selector)

	for {
		SQL, err := s.prepareSQL(view, selector, upstream, params, batchData)
		if err != nil {
			return err
		}

		readData, err = s.flush(ctx, db, SQL, collector, batchData)
		if err != nil {
			return err
		}

		batchData.CurrentlyRead = batchData.CurrentlyRead + readData

		if batchData.BatchReadSize == 0 || batchData.CurrentlyRead == limit || readData < batchData.BatchReadSize {
			break
		}

		readData = 0
	}
	return nil
}

func (s *Service) flush(ctx context.Context, db *sql.DB, SQL string, collector *data.Collector, batchData *BatchData) (int, error) {
	reader, err := read.New(ctx, db, SQL, collector.NewItem(), io.Resolve(collector.Resolve))
	if err != nil {
		return 0, err
	}

	visitor := collector.Visitor()

	readData := 0
	err = reader.QueryAll(ctx, func(row interface{}) error {
		readData++
		return visitor(row)
	}, batchData.Placeholders...)

	if err != nil {
		return 0, nil
	}

	return readData, nil
}

func (s *Service) prepareSQL(view *data.View, selector *data.Selector, upstream rdata.Map, params rdata.Map, batchData *BatchData) (string, error) {
	SQL, err := s.sqlBuilder.Build(view, selector, batchData)
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

func (s *Service) readRelations(ctx context.Context, session *data.Session, collector *data.Collector, params rdata.Map, wg *sync.WaitGroup) error {
	collectorChildren := collector.Relations()
	for i := range collectorChildren {
		if err := s.readAll(ctx, session, collectorChildren[i], params, wg); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) buildViewParams(ctx context.Context, session *data.Session, view *data.View) (rdata.Map, error) {
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
		default:
			return nil, fmt.Errorf("unsupported location kind %v", param.In.Kind)
		}
	}

	return params, nil
}

//Apply configures Service
func (s *Service) Apply(options Options) {
	for i := 0; i < len(options); i++ {
		switch actual := options[i].(type) {
		case AllowUnmapped:
			s.AllowUnmapped = actual
		}
	}
}

func (s *Service) addViewParams(ctx context.Context, paramMap rdata.Map, param *data.Parameter, session *data.Session) error {
	view := param.View()
	destSlice := reflect.New(view.Schema.SliceType()).Interface()

	collector := view.Collector(false, destSlice, false)
	if err := s.readAll(ctx, session, collector, paramMap, nil); err != nil {
		return err
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
		value := view.ParamField.Interface(holderPtr)

		paramMap.SetValue(param.Name, value)
	default:
		return fmt.Errorf("parameter %v return more than one value", param.Name)
	}

	return nil
}

//New creates Service instance
func New(resource *data.Resource) *Service {
	return &Service{
		sqlBuilder: NewBuilder(),
		Resource:   resource,
	}
}
