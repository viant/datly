package reader

import (
	"context"
	"database/sql"
	"github.com/viant/datly/v1/config"
	"github.com/viant/datly/v1/connection"
	"github.com/viant/datly/v1/data"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

//Service represents reader service
type Service struct {
	connection      *connection.Service
	sqlBuilder      *Builder
	computeMetadata bool
}

// TODO: ReadUnmapped
// TODO: support option selector
// TODO: get columns for SQL
// TODO: Compute metadata () len(columns)
// TODO: control option deliver selection with respect column that can be queryable projected
// TODO: update view columns with metadata columns

//Read select data from database based on View and assign it to dest. Dest has to be pointer.
//It is possible to create type based on database column types - in that case, dest has to be pointer to interface{}
func (s *Service) Read(ctx context.Context, view *data.View, dest interface{}) error {
	db := s.connection.Connection(view.Connector)
	columns, dataType, err := s.metadata(view, db)
	if err != nil {
		return err
	}

	ensuredDest := s.ensureDest(dest, dataType)
	SQL := s.sqlBuilder.Build(columns, view.Table)

	appender := s.appender(ensuredDest)
	reader, err := read.New(ctx, db, SQL, func() interface{} {
		return appender.Add()
	})

	if err != nil {
		return err
	}

	err = reader.QueryAll(ctx, func(row interface{}) error {
		return nil
	})

	if err != nil {
		return err
	}

	destWasNilInterface := dest != ensuredDest
	if destWasNilInterface {
		*dest.(*interface{}) = ensuredDest
	}
	return nil
}

func (s *Service) metadata(view *data.View, db *sql.DB) ([]string, reflect.Type, error) {
	var columnNames []string
	var dataType reflect.Type
	var err error

	if view.Columns != nil && len(view.Columns) != 0 {
		columnNames = DataColumnsToNames(view.Columns)
	}

	if view.Component != nil {
		dataType = view.Component.ComponentType()
	}

	if columnNames == nil || dataType == nil {
		var detectedTypes []*sql.ColumnType
		detectedTypes, err = s.detectColumns(db, view.Table)
		if err != nil {
			return nil, nil, err
		}
		filteredColumns := s.filterColumns(detectedTypes, view)
		columnNames = make([]string, len(filteredColumns))
		for i := range filteredColumns {
			columnNames[i] = filteredColumns[i].Name()
		}
		dataType = TypeOf(io.TypesToColumns(filteredColumns))
	}

	return columnNames, dataType, err
}

func (s *Service) appender(dest interface{}) *xunsafe.Appender {
	structType := s.actualStructType(dest)
	slice := xunsafe.NewSlice(structType, xunsafe.UseItemAddrOpt(false))
	appender := slice.Appender(unsafe.Pointer(reflect.ValueOf(dest).Pointer()))
	return appender
}

func (s *Service) actualStructType(dest interface{}) reflect.Type {
	rType := reflect.TypeOf(dest).Elem() // Dest has to be a pointer
	if rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType
}

func (s *Service) detectColumns(db *sql.DB, tableName string) ([]*sql.ColumnType, error) {
	rows, err := db.Query("SELECT * FROM " + tableName + " WHERE 1=2")
	if err != nil {
		return nil, err
	}

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err

	}
	return types, nil
}

func (s *Service) ensureDest(dest interface{}, dataType reflect.Type) interface{} {
	valueOf := reflect.ValueOf(dest)
	if valueOf.Elem().Interface() != nil {
		return dest
	}
	return reflect.New(reflect.SliceOf(dataType)).Interface()
}

func (s *Service) filterColumns(types []*sql.ColumnType, view *data.View) []*sql.ColumnType {
	columns := make(map[string]bool)
	for _, column := range view.Selector.ExcludedColumns {
		columns[strings.ToLower(column)] = true
	}

	filteredTypes := make([]*sql.ColumnType, 0)
	for i := range types {
		if val, ok := columns[strings.ToLower(types[i].Name())]; ok && val {
			continue
		}
		filteredTypes = append(filteredTypes, types[i])
	}
	return filteredTypes
}

//Apply configures Service
func (s *Service) Apply(options Options) {
}

//New creates Service instance
func New(connectors ...*config.Connector) (*Service, error) {
	conn, err := connection.New(connectors...)
	if err != nil {
		return nil, err
	}
	return &Service{
		connection: conn,
		sqlBuilder: NewBuilder(),
	}, nil
}
