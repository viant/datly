package router

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/metadata/sink"
	"reflect"
	"strings"
	"time"
)

var asyncRecordMatcher *io.Matcher

const AsyncStateRunning = "RUNNING"
const AsyncStateDone = "DONE"

type (
	Async struct {
		TableName     string
		EnsureDBTable bool
		Connector     *view.Connector
		Qualifier     string
		ExpiryTimeInS int

		_qualifier *qualifier
		_updater   *update.Service
		_inserter  *insert.Service
	}

	qualifier struct {
		parameter *view.Parameter
		accessor  *types.Accessor
	}

	AsyncRecord struct {
		JobID        string `sqlx:"primaryKey=true"`
		Qualifier    *string
		State        string
		Value        *string
		Error        *string
		CreationTime time.Time
		EndTime      *time.Time
	}
)

func (a *Async) Init(ctx context.Context, resource *view.Resource) error {
	if a.TableName == "" {
		return fmt.Errorf("async TableName can't be empty")
	}

	if a.Connector == nil {
		return fmt.Errorf("async connector can't be empty")
	}

	if err := a.Connector.Init(ctx, resource.GetConnectors()); err != nil {
		return err
	}

	if err := a.initAccessor(resource); err != nil {
		return nil
	}

	if err := a.ensureTable(ctx); err != nil {
		return err
	}

	if err := a.ensureServices(ctx); err != nil {
		return err
	}

	return nil
}

func (a *Async) initAccessor(resource *view.Resource) error {
	if a.Qualifier == "" {
		return nil
	}

	dotIndex := strings.Index(a.Qualifier, ".")
	var paramName, path string
	if dotIndex != -1 {
		paramName, path = a.Qualifier[:dotIndex], a.Qualifier[dotIndex+1:]
	} else {
		paramName = a.Qualifier
	}

	param, err := resource.ParamByName(paramName)
	if err != nil {
		return err
	}

	var accessor *types.Accessor
	if path != "" {
		accessors := types.NewAccessors(&types.VeltyNamer{})
		accessors.InitPath(param.ActualParamType(), path)
		accessor, err = accessors.AccessorByName(path)

		if err != nil {
			return err
		}
	}

	a._qualifier = &qualifier{
		parameter: param,
		accessor:  accessor,
	}

	return nil
}

func (a *Async) ensureTable(ctx context.Context) error {
	db, err := a.Connector.DB()
	if err != nil {
		return err
	}

	return a.ensureDBTableMatches(ctx, db)
}

func (a *Async) ensureDBTableMatches(ctx context.Context, db *sql.DB) error {
	session, err := config.Session(ctx, db)
	if err != nil {
		return err
	}

	columns, err := config.Columns(ctx, session, db, a.TableName)
	if err == nil && len(columns) > 0 {
		return a.checkTypeMatch(columns)
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}

	createTableSQL := fmt.Sprintf(mysqlCreateTable, a.TableName)
	_, err = tx.Exec(createTableSQL)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (a *Async) checkTypeMatch(columns []sink.Column) error {
	ioColumns := make([]io.Column, 0, len(columns))
	for _, column := range columns {
		ioColumns = append(ioColumns, io.NewColumn(column.Name, column.Type, nil))
	}

	_, err := asyncRecordMatcher.Match(reflect.TypeOf(AsyncRecord{}), ioColumns)
	return err
}

func (a *Async) ensureServices(ctx context.Context) error {
	db, err := a.Connector.DB()
	if err != nil {
		return err
	}

	a._inserter, err = insert.New(ctx, db, a.TableName)
	if err != nil {
		return err
	}

	a._updater, err = update.New(ctx, db, a.TableName)
	if err != nil {
		return err
	}

	return nil
}
