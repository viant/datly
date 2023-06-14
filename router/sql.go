package router

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/executor/sequencer"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/validator"
	"github.com/viant/xunsafe"
	"reflect"
)

type (
	SqlxService struct {
		toExecute        []interface{}
		executablesIndex expand.ExecutablesIndex
		options          *sqlx.Options

		validator  validator.Service
		db         *sql.DB
		dialect    *info.Dialect
		connectors view.Connectors
	}

	SqlxIterator struct {
		toExecute []interface{}
		index     int
	}
)

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

func (s *SqlxService) Flush(ctx context.Context, tableName string) error {
	if len(s.toExecute) > 0 {
		var options []interface{}
		if s.options.WithTx != nil {
			options = append(options, s.options.WithTx)
		}

		exec := executor.New()
		if err := exec.ExecuteStmts(ctx, s, &SqlxIterator{toExecute: s.toExecute}); err != nil {
			return err
		}
	}

	return nil
}

func (s *SqlxService) Insert(tableName string, data interface{}) error {
	s.appendExecutable(expand.ExecTypeInsert, tableName, data)
	return nil
}

func (s *SqlxService) appendExecutable(execType expand.ExecType, tableName string, data interface{}) {
	executable := &expand.Executable{
		Table:    tableName,
		ExecType: expand.ExecTypeInsert,
		Data:     data,
		IsLast:   true,
	}

	s.executablesIndex.UpdateLastExecutable(execType, tableName, executable)
	s.toExecute = append(s.toExecute, executable)
}

func (s *SqlxService) Update(tableName string, data interface{}) error {
	s.appendExecutable(expand.ExecTypeUpdate, tableName, data)
	return nil
}

func (s *SqlxService) Delete(tableName string, data interface{}) error {
	s.appendExecutable(expand.ExecTypeDelete, tableName, data)
	return nil
}

func (s *SqlxService) Execute(DML string, options ...sqlx.ExecutorOption) error {
	opts := &sqlx.ExecutorOptions{}
	for _, option := range options {
		option(opts)
	}

	s.toExecute = append(s.toExecute, &executor.SQLStatment{
		SQL:  DML,
		Args: opts.Args,
	})

	return nil
}

func (s *SqlxService) Read(ctx context.Context, dest interface{}, SQL string, params ...interface{}) error {
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

func (s *SqlxService) Db(ctx context.Context) (*sql.DB, error) {
	if s.db != nil {
		return s.db, nil
	}

	db, err := s.openDBConnection()
	s.db = db

	return db, err
}

func (s *SqlxService) openDBConnection() (*sql.DB, error) {
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

	return nil, fmt.Errorf("unspecified DB source")
}

func (s *SqlxService) Tx(ctx context.Context) (*sql.Tx, error) {
	db, err := s.Db(ctx)
	if err != nil {
		return nil, err
	}

	return db.BeginTx(ctx, nil)
}

func (s *SqlxService) Validator() validator.Service {
	return s.validator
}

func (s *SqlxService) Dialect(ctx context.Context) (*info.Dialect, error) {
	if s.dialect != nil {
		return s.dialect, nil
	}

	dialect, err := s.getDialect(ctx)
	s.dialect = dialect
	return dialect, err
}

func (s *SqlxService) getDialect(ctx context.Context) (*info.Dialect, error) {
	db, err := s.Db(ctx)
	if err != nil {
		return nil, err
	}

	return config.Dialect(ctx, db)
}

func (s *SqlxService) CanBatch(table string) bool {
	//TODO: handle it properly
	return false
}

func (s *SqlxService) Allocate(ctx context.Context, tableName string, dest interface{}, selector string) error {
	db, err := s.Db(ctx)
	if err != nil {
		return err
	}

	service := sequencer.New(context.Background(), db)
	return service.Next(tableName, dest, selector)
}

func (s *SqlxService) CanBatchGlobally() bool {
	return false
}
