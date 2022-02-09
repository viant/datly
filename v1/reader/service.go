package reader

import (
	"context"
	"database/sql"
	"github.com/viant/datly/v1/data"
	"github.com/viant/datly/v1/utils"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

//Service represents reader service
type Service struct {
	metaService   *data.Service
	views         []*data.View
	sqlBuilder    *Builder
	AllowUnmapped AllowUnmapped
}

type Views struct {
	views []*data.View
}

//TODO: batch table records
//Read select data from database based on View and assign it to dest. Dest has to be pointer.
//TODO: Select with join when connector is the same for one to one relation
func (s *Service) Read(ctx context.Context, session *Session) error {
	err := session.View.IsValid()
	if err != nil {
		return err
	}

	result, err := s.collectData(ctx, session)
	if err != nil {
		return err
	}

	if dest, ok := session.Dest.(*interface{}); ok {
		*dest = result
	}

	return nil
}

func (s *Service) appender(dest interface{}) *xunsafe.Appender {
	strType := s.actualStructType(dest)
	slice := xunsafe.NewSlice(strType)
	appender := slice.Appender(xunsafe.AsPointer(dest))
	return appender
}

func (s *Service) actualStructType(dest interface{}) reflect.Type {
	rType := reflect.TypeOf(dest).Elem()
	if rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType
}

func (s *Service) ensureDest(dest interface{}, dataType reflect.Type) interface{} {
	if _, ok := dest.(*interface{}); ok {
		return s.dest(dataType)
	}

	return dest
}

func (s *Service) collectData(ctx context.Context, session *Session) (interface{}, error) {
	dataType := session.DataType()
	ensuredDest := s.ensureDest(session.Dest, dataType)
	resolver := s.sessionResolver(session)
	db, err := s.metaService.Connection(session.View.Connector)
	if err != nil {
		return nil, err
	}

	err = s.read(ctx, session, db, ensuredDest, resolver.Resolve)
	if err != nil {
		return nil, err
	}

	if len(session.View.References) == 0 {
		return ensuredDest, nil
	}

	collector := NewCollector(session.View.References, resolver, ensuredDest)
	err = s.collectRefViews(ctx, session, collector, err)
	if err != nil {
		return nil, err
	}

	return ensuredDest, nil
}

func (s *Service) collectRefViews(ctx context.Context, session *Session, collector *Collector, err error) error {
	wg := sync.WaitGroup{}
	dataSize := len(session.View.References)
	wg.Add(dataSize)
	errors := utils.NewErrors(dataSize)
	for i := range session.View.References {
		iCoppy := i
		go func() {
			defer wg.Done()
			// TODO: use resolver column values and pass them as sql placeholders
			db, err := s.metaService.Connection(session.View.References[iCoppy].Child.Connector)
			if err != nil {
				errors.AddError(err, iCoppy+1)
				return
			}
			visitor := func(row interface{}) {
				collector.Collect(row, session.View.References[iCoppy].RefHolder)
			}
			errors.AddError(s.readRefView(ctx, session.View.References[iCoppy].Child, db, visitor), iCoppy)
		}()
	}

	wg.Wait()
	err = errors.Error()
	return err
}

func (s *Service) dest(rType reflect.Type) interface{} {
	return reflect.New(reflect.SliceOf(rType)).Interface()
}

func (s *Service) read(ctx context.Context, session *Session, db *sql.DB, dest interface{}, resolve io.Resolve) error {
	SQL := s.sqlBuilder.Build(session.View, session.SelectorInUse())

	appender := s.appender(dest)
	reader, err := read.New(ctx, db, SQL, func() interface{} {
		return appender.Add()
	}, resolve)

	if err != nil {
		return err
	}

	err = reader.QueryAll(ctx, func(row interface{}) error {
		return nil
	})

	return err
}

func (s *Service) readRefView(ctx context.Context, view *data.View, db *sql.DB, visitor func(row interface{})) error {
	SQL := s.sqlBuilder.Build(view, view.Default)

	reader, err := read.New(ctx, db, SQL, func() interface{} {
		return reflect.New(view.DataType().Elem()).Interface()
	})

	if err != nil {
		return err
	}

	err = reader.QueryAll(ctx, func(row interface{}) error {
		visitor(row)
		return nil
	})

	return err
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

func (s *Service) sessionResolver(session *Session) *Resolver {
	if s.AllowUnmapped {
		return NewResolver(nil)
	}

	allowedColumns := make([]string, len(session.View.References))
	for i := range session.View.References {
		allowedColumns[i] = session.View.References[i].Column
	}
	return NewResolver(allowedColumns)
}

//New creates Service instance
func New(service *data.Service) *Service {
	return &Service{
		metaService: service,
		sqlBuilder:  NewBuilder(),
	}
}
