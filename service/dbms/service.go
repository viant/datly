package dbms

import (
	"context"
	"fmt"
	"github.com/viant/datly/service/dbms/provider"
	"github.com/viant/datly/service/dbms/provider/bigquery"
	"github.com/viant/datly/service/dbms/provider/mysql"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/sink"
	"reflect"
	"sync"
	"unsafe"
)

// TODO shall this service to create/checks table move to sqlx as service ?
type (
	Service struct {
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
		sqlSource provider.SqlSource
	}

	TableConfig struct {
		RecordType     reflect.Type
		TableName      string
		Dataset        string
		CreateIfNeeded bool
		GenerateAutoPk bool
	}
)

func (c *Service) EnsureTable(ctx context.Context, connector *view.Connector, cfg *TableConfig) (provider.SqlSource, error) {
	schema, err := c.loadSchema(ctx, connector, cfg)
	if err != nil {
		return nil, err
	}
	return schema.sqlSource, schema.err
}

func (c *Service) loadSchema(ctx context.Context, connector *view.Connector, cfg *TableConfig) (*schema, error) {
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

func (c *Service) getSchema(ctx context.Context, connector *view.Connector, cfg *TableConfig) (*schema, error) {
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
	//TODO change tag name - this functionality is too generic for async use case
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

func (c *Service) checkTypeMatch(columns []sink.Column, recordType reflect.Type) error {
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

func (c *Service) getMatcher(recordType reflect.Type) (*io.Matcher, error) {
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

func (c *Service) sqlSource(connector *view.Connector, cfg *TableConfig) (provider.SqlSource, error) {
	switch connector.Driver {
	case "mysql":
		return mysql.NewSQLSource(), nil
	case "bigquery":
		return bigquery.NewSQLSource(cfg.Dataset)
	}
	return nil, fmt.Errorf("unsupported async database %v", connector.Driver)
}

// New creates dbms service
func New() *Service {
	return &Service{
		Mutex:         sync.Mutex{},
		schemaChecked: map[dbKey]*schema{},
		matchers:      sync.Map{},
	}
}
