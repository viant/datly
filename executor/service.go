package executor

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/insert/batcher"
	"github.com/viant/sqlx/io/update"
	"reflect"
	"strings"
)

type (
	Executor struct {
		sqlBuilder *SqlBuilder
	}
)

func New() *Executor {
	return &Executor{
		sqlBuilder: NewBuilder(),
	}
}

//TODO: remove reflection
//TODO: customize global batch collector
func (e *Executor) Exec(ctx context.Context, session *Session) error {
	state, data, err := e.sqlBuilder.Build(session.View, session.Lookup(session.View))
	if err != nil {
		return err
	}

	session.State = state.State
	if err = e.exec(ctx, session, data, state.DataUnit); err != nil {
		return err
	}

	return state.Flush()
}

func (e *Executor) exec(ctx context.Context, session *Session, data []*SQLStatment, criteria *expand.DataUnit) error {
	if len(data) == 0 {
		return nil
	}

	canBeBatchedGlobally := false
	db, err := session.View.Db()
	if err != nil {
		return err
	}

	aTx := newLazyTx(db, canBeBatchedGlobally)

	for i := range data {
		if err = e.execData(ctx, aTx, data[i], session, criteria, db, canBeBatchedGlobally); err != nil {
			_ = aTx.RollbackIfNeeded()
			return err
		}
	}

	return aTx.CommitIfNeeded()
}

func (e *Executor) canBeBatchedGlobally(criteria *expand.DataUnit, data []*SQLStatment) bool {
	executables := criteria.FilterExecutables(extractStatements(data), true)
	if len(executables) != len(data) {
		return false
	}

	tableNamesIndex := map[string]bool{}
	for _, executable := range executables {
		if executable.ExecType == expand.ExecTypeUpdate {
			return false
		}

		tableNamesIndex[executable.Table] = true
	}

	return len(tableNamesIndex) == 1
}

func extractStatements(data []*SQLStatment) []string {
	result := make([]string, 0, len(data))
	for _, datum := range data {
		result = append(result, datum.SQL)
	}

	return result
}

func (e *Executor) execData(ctx context.Context, aTx *lazyTx, data *SQLStatment, session *Session, criteria *expand.DataUnit, db *sql.DB, canBeBatchedGlobally bool) error {
	if strings.TrimSpace(data.SQL) == "" {
		return nil
	}

	if executable, ok := criteria.IsServiceExec(data.SQL); ok {
		switch executable.ExecType {
		case expand.ExecTypeInsert:
			return e.handleInsert(ctx, aTx, session, executable, db, canBeBatchedGlobally)
		case expand.ExecTypeUpdate:
			return e.handleUpdate(ctx, aTx, db, executable)
		default:
			return fmt.Errorf("unsupported exec type: %v\n", executable.ExecType)
		}
	}

	tx, err := aTx.Tx()
	if err != nil {
		return err
	}

	return e.executeStatement(ctx, tx, data, session)
}

func (e *Executor) handleUpdate(ctx context.Context, aTx *lazyTx, db *sql.DB, executable *expand.Executable) error {
	service, err := update.New(ctx, db, executable.Table)
	if err != nil {
		return err
	}

	options, err := aTx.PrepareTxOptions()
	if err != nil {
		return err
	}

	_, err = service.Exec(ctx, executable.Data, options...)
	return err
}

func (e *Executor) handleInsert(ctx context.Context, aTx *lazyTx, session *Session, executable *expand.Executable, db *sql.DB, canBeBatchedGlobally bool) error {
	dialect, err := session.View.Connector.Dialect(ctx)
	if err != nil {
		return err
	}

	canBeBatched := session.View.TableBatches[executable.Table] && e.dialectSupportsBatching(ctx, session.View)

	service, err := insert.New(ctx, db, executable.Table)
	if err != nil {
		return err
	}

	if !canBeBatched {
		tx, err := aTx.Tx()
		if err != nil {
			return err
		}

		_, _, err = service.Exec(ctx, executable.Data, tx, dialect)
		return err
	}

	collection := session.Collection(executable)
	collection.Append(executable.Data)
	if !executable.IsLast {
		return nil
	}

	if !canBeBatchedGlobally {
		options, err := aTx.PrepareTxOptions()
		if err != nil {
			return err
		}

		_, _, err = service.Exec(ctx, collection.Unwrap(), append(options, dialect)...)
		return err
	}

	aBatcher, err := batcherRegistry.GetBatcher(executable.Table, reflect.TypeOf(executable.Data), db, &batcher.Config{
		MaxElements:   100,
		MaxDurationMs: 10,
		BatchSize:     100,
	})

	if err != nil {
		return err
	}

	//TODO: remove reflection
	rSlice := reflect.ValueOf(collection.Unwrap()).Elem()
	sliceLen := rSlice.Len()
	var state *batcher.State
	for i := 0; i < sliceLen; i++ {
		state, err = aBatcher.Collect(rSlice.Index(i).Interface())
		if err != nil {
			return err
		}
	}

	if state != nil {
		return state.Wait()
	}

	return nil
}

func (e *Executor) dialectSupportsBatching(ctx context.Context, aView *view.View) bool {
	dialect, err := aView.Connector.Dialect(ctx)
	return err == nil && dialect.Insert.MultiValues()
}

func (e *Executor) executeStatement(ctx context.Context, tx *sql.Tx, stmt *SQLStatment, session *Session) error {
	_, err := tx.ExecContext(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		session.View.Logger.LogDatabaseErr(stmt.SQL, err)
		err = fmt.Errorf("error occured while connecting to database")
	}

	return err
}
