package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/service/executor"
	expand2 "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/executor/sequencer"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/validator"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
)

type (
	Service struct {
		dataUnit *expand2.DataUnit
		options  *sqlx.Options

		validator *validator.Service
		db        *sql.DB
		dialect   *info.Dialect

		mainConnector *view.Connector
		connectors    view.Connectors

		request    *http.Request
		txNotifier func(tx *sql.Tx)
		tx         *sql.Tx
	}

	SqlxIterator struct {
		toExecute []interface{}
		index     int
	}
)

func (s *Service) Load(ctx context.Context, tableName string, data interface{}) error {
	panic("function not yet implemented")
}

func (s *SqlxIterator) HasNext() bool {
	return s.index < len(s.toExecute)
}

func (s *SqlxIterator) Next() interface{} {
	actual := s.index
	s.index++
	return s.toExecute[actual]
}

func (s *SqlxIterator) HasAny() bool {
	return len(s.toExecute) > 0
}

func (s *Service) Flush(ctx context.Context, tableName string) error {
	var options []executor.DBOption
	tx := s.options.WithTx
	if tx == nil {
		var err error
		tx, err = s.Tx(ctx)
		if err != nil {
			return err
		}
	}

	options = append(options, executor.WithTx(tx))

	exec := executor.New()
	if err := exec.ExecuteStmts(ctx, s, &SqlxIterator{
		toExecute: s.dataUnit.Statements.FilterByTableName(tableName),
	}, options...); err != nil {
		return err
	}

	return nil
}

func (s *Service) Insert(tableName string, data interface{}) error {
	_, err := s.dataUnit.Insert(data, tableName)
	return err
}

func (s *Service) Update(tableName string, data interface{}) error {
	_, err := s.dataUnit.Update(data, tableName)
	return err
}

func (s *Service) Delete(tableName string, data interface{}) error {
	_, err := s.dataUnit.Delete(data, tableName)
	return err
}

func (s *Service) Execute(DML string, options ...sqlx.ExecutorOption) error {
	opts := &sqlx.ExecutorOptions{}
	for _, option := range options {
		option(opts)
	}

	s.dataUnit.Statements.Execute(&expand2.SQLStatment{
		SQL:  DML,
		Args: opts.Args,
	})

	return nil
}

func (s *Service) Read(ctx context.Context, dest interface{}, SQL string, params ...interface{}) error {
	dstType := reflect.TypeOf(dest)
	dstElemType := dstType

	if dstElemType.Kind() != reflect.Ptr {
		return fmt.Errorf("dest has to be *[]struct or *[]*struct but was %v", dstType.String())
	}

	dstElemType = dstElemType.Elem()
	if dstElemType.Kind() != reflect.Slice {
		return fmt.Errorf("dest has to be *[]struct or *[]*struct but was %v", dstType.String())
	}

	xSlice := xunsafe.NewSlice(dstElemType)
	ptr := xunsafe.AsPointer(dest)
	appender := xSlice.Appender(ptr)
	db, err := s.Db(ctx)
	if err != nil {
		return err
	}

	reader, err := read.New(ctx, db, SQL, func() interface{} {
		return appender.Add()
	})

	if err != nil {
		return err
	}

	return reader.QueryAll(ctx, func(row interface{}) error {
		return nil
	}, params...)
}

func (s *Service) Db(ctx context.Context) (*sql.DB, error) {
	if s.db != nil {
		return s.db, nil
	}

	db, err := s.openDBConnection()
	s.db = db

	return db, err
}

func (s *Service) openDBConnection() (*sql.DB, error) {
	if s.options.WithConnector != "" {
		connector, err := s.connectors.Lookup(s.options.WithConnector)
		if err != nil {
			return nil, err
		}

		db, err := connector.DB()
		return db, err
	}

	if s.options.WithDb != nil {
		return s.options.WithDb, nil
	}

	if s.mainConnector != nil {
		return s.mainConnector.DB()
	}

	return nil, fmt.Errorf("unspecified DB source")
}

func (s *Service) Tx(ctx context.Context) (*sql.Tx, error) {
	if s.tx != nil {
		return s.tx, nil
	}

	db, err := s.Db(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := db.BeginTx(ctx, nil)
	s.tx = tx

	if tx != nil && s.txNotifier != nil {
		s.txNotifier(tx)
	}

	return tx, err
}

func (s *Service) Validator() *validator.Service {
	return s.validator
}

func (s *Service) Dialect(ctx context.Context) (*info.Dialect, error) {
	if s.dialect != nil {
		return s.dialect, nil
	}

	dialect, err := s.getDialect(ctx)
	s.dialect = dialect
	return dialect, err
}

func (s *Service) getDialect(ctx context.Context) (*info.Dialect, error) {
	db, err := s.Db(ctx)
	if err != nil {
		return nil, err
	}

	return config.Dialect(ctx, db)
}

func (s *Service) CanBatch(table string) bool {
	//TODO: handle it properly
	return false
}

func (s *Service) Allocate(ctx context.Context, tableName string, dest interface{}, selector string) error {
	db, err := s.Db(ctx)
	if err != nil {
		return err
	}

	service := sequencer.New(context.Background(), db)
	return service.Next(tableName, dest, selector)
}

func (s *Service) CanBatchGlobally() bool {
	return false
}
