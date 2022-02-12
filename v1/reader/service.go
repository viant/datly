package reader

import (
	"context"
	"fmt"
	"github.com/viant/datly/v1/data"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"reflect"
)

//Service represents reader service
type Service struct {
	views         []*data.View
	sqlBuilder    *Builder
	AllowUnmapped AllowUnmapped
}

type Views struct {
	views []*data.View
}

//TODO: batch table records
//Read select data from database based on View and assign it to dest. ParentDest has to be pointer.
//TODO: Select with join when connector is the same for one to one relation
func (s *Service) Read(ctx context.Context, session *data.Session) error {
	session.Allocate()

	if isSlice(session.Dest) {
		session.ViewsDest()[0] = session.Dest
	}

	err := s.read(ctx, session, session.View, nil)
	if err != nil {
		return err
	}

	if dest, ok := session.Dest.(*interface{}); ok {
		*dest = session.ViewsDest()[0]
	}

	return nil
}

func isSlice(dest interface{}) bool {
	_, err := sliceType(reflect.TypeOf(dest))
	return err == nil
}

func sliceType(rType reflect.Type) (reflect.Type, error) {
	switch rType.Kind() {
	case reflect.Ptr:
		return sliceType(rType.Elem())
	case reflect.Slice:
		return rType, nil
	}
	return nil, fmt.Errorf("invalid type %v", rType.String())
}

func (s *Service) actualStructType(dest interface{}) reflect.Type {
	rType := reflect.TypeOf(dest).Elem()
	if rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType
}

func (s *Service) read(ctx context.Context, session *data.Session, view *data.View, parent *data.Collector) error {
	db, err := view.Db()
	if err != nil {
		return err
	}

	SQL, err := s.sqlBuilder.Build(view, session.Selectors.Lookup(view.Name))
	if err != nil {
		return err
	}

	collector := view.Collector(session)

	reader, err := read.New(ctx, db, SQL, collector.NewItem(session, view), io.Resolve(collector.Resolve))

	if err != nil {
		return err
	}

	visitor := collector.Visitor(session, view)
	if parent != nil {
		visitor = parent.Visitor(session, view)
	}
	err = reader.QueryAll(ctx, visitor)

	if len(view.With) == 0 {
		return nil
	}

	for i := range view.With {
		if err := s.read(ctx, session, &view.With[i].Of.View, collector); err != nil {
			return err
		}
	}

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

//New creates Service instance
func New() *Service {
	return &Service{
		sqlBuilder: NewBuilder(),
	}
}
