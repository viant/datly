package async

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/gateway/router/async/db"
	"github.com/viant/datly/gateway/router/async/db/bigquery"
	"github.com/viant/datly/gateway/router/async/db/mysql"
	"github.com/viant/datly/gateway/router/async/handler/s3"
	"github.com/viant/datly/gateway/router/async/handler/sqs"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/sink"
	async2 "github.com/viant/xdatly/handler/async"
	"reflect"
	"sync"
	"unsafe"
)

type (
	Async struct {
		db       Db
		handlers RecordHandler
	}

	Db struct {
		sync.Mutex
		schemaChecked map[dbKey]*schema
		matchers      sync.Map
	}

	dbKey struct {
		connectorName string
		tableName     string
		recordType    reflect.Type
	}

	schema struct {
		err       error
		sqlSource db.SqlSource
	}

	TableConfig struct {
		RecordType     reflect.Type
		TableName      string
		Dataset        string
		CreateIfNeeded bool
		GenerateAutoPk bool
	}

	RecordHandler struct {
		sync.Map
	}

	singletonHandler struct {
		sync.Once
		err     error
		handler Handler
	}
)

func NewChecker() *Async {
	return &Async{
		db: Db{
			Mutex:         sync.Mutex{},
			schemaChecked: map[dbKey]*schema{},
			matchers:      sync.Map{},
		},
	}
}

func (c *Async) EnsureTable(ctx context.Context, connector *view.Connector, cfg *TableConfig) (db.SqlSource, error) {
	schema, err := c.db.loadSchema(ctx, connector, cfg)
	if err != nil {
		return nil, err
	}

	return schema.sqlSource, schema.err
}

func (c *Db) loadSchema(ctx context.Context, connector *view.Connector, cfg *TableConfig) (*schema, error) {
	key := dbKey{
		connectorName: connector.Name,
		tableName:     cfg.TableName,
		recordType:    cfg.RecordType,
	}

	c.Lock()
	defer c.Unlock()
	schema, ok := c.schemaChecked[key]
	if ok {
		return schema, nil
	}

	schema, err := c.getSchema(ctx, connector, cfg)
	if err != nil {
		return nil, err
	}

	c.schemaChecked[key] = schema
	return schema, err
}

func (c *Db) getSchema(ctx context.Context, connector *view.Connector, cfg *TableConfig) (*schema, error) {
	aDb, err := connector.DB()
	if err != nil {
		return nil, err
	}

	session, err := config.Session(ctx, aDb)
	if err != nil {
		return nil, err
	}

	columns, err := config.Columns(ctx, session, aDb, cfg.TableName)
	if err == nil && len(columns) > 0 {
		return &schema{}, nil
	}

	if !cfg.CreateIfNeeded {
		return &schema{err: fmt.Errorf("table %v doesn't exists", cfg.TableName)}, nil
	}

	sqlSource, err := c.sqlSource(connector, cfg)
	if err != nil {
		return nil, err
	}

	asyncTable, err := sqlSource.CreateTable(cfg.RecordType, cfg.TableName, view.AsyncTagName, cfg.GenerateAutoPk)
	if err != nil {
		return nil, err
	}

	tx, err := aDb.BeginTx(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	_, err = tx.Exec(asyncTable.SQL)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &schema{
		err:       err,
		sqlSource: sqlSource,
	}, nil
}

func (c *Db) checkTypeMatch(columns []sink.Column, recordType reflect.Type) error {
	for recordType.Kind() == reflect.Slice {
		recordType = recordType.Elem()
	}

	ioColumns := make([]io.Column, 0, len(columns))
	for _, column := range columns {
		ioColumns = append(ioColumns, io.NewColumn(column.Name, column.Type, nil))
	}

	matcher, err := c.getMatcher(recordType)
	if err != nil {
		return err
	}

	_, err = matcher.Match(recordType, ioColumns)
	return err
}

func (c *Db) getMatcher(recordType reflect.Type) (*io.Matcher, error) {
	actualType := recordType
	value, ok := c.matchers.Load(actualType)
	if ok {
		return value.(*io.Matcher), nil
	}

	for recordType.Kind() == reflect.Slice {
		recordType = recordType.Elem()
	}

	matcher := io.NewMatcher("sqlx", func(column io.Column) func(pointer unsafe.Pointer) interface{} {
		return func(pointer unsafe.Pointer) interface{} {
			return reflect.New(column.ScanType()).Elem().Interface()
		}
	})

	c.matchers.Store(actualType, matcher)
	return matcher, nil
}

func (c *Db) sqlSource(connector *view.Connector, cfg *TableConfig) (db.SqlSource, error) {
	switch connector.Driver {
	case "mysql":
		return mysql.NewSQLSource(), nil
	case "bigquery":
		return bigquery.NewSQLSource(cfg.Dataset)
	}
	return nil, fmt.Errorf("unsupported async database %v", connector.Driver)
}

func (c *Async) Handler(ctx context.Context, cfg *async2.Notification) (Handler, error) {
	return c.handlers.loadHandler(ctx, cfg)
}

func (a *RecordHandler) loadHandler(ctx context.Context, cfg *async2.Notification) (Handler, error) {
	actual, _ := a.LoadOrStore(*cfg, &singletonHandler{})
	aHandler := actual.(*singletonHandler)
	aHandler.Once.Do(func() {
		aHandler.handler, aHandler.err = a.detectHandlerType(ctx, cfg)
	})

	return aHandler.handler, aHandler.err
}

func (a *RecordHandler) detectHandlerType(ctx context.Context, cfg *async2.Notification) (Handler, error) {
	switch cfg.Method {
	case async2.NotificationMethodStorage:
		return s3.NewHandler(ctx, cfg.Destination)
	case async2.NotificationMethodMessageBus:
		return sqs.NewHandler(ctx, "datly-jobs")

	case async2.NotificationMethodUndefined:
		switch env.BuildType {
		case env.BuildTypeKindLambda:
			return sqs.NewHandler(ctx, "datly-async")
		default:
			return nil, nil
		}

	default:
		return nil, fmt.Errorf("unsupported async HandlerType %v", cfg.Method)
	}
}
