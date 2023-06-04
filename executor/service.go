package executor

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/insert/batcher"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"reflect"
	"strings"
)

type (
	Executor struct {
		sqlBuilder *SqlBuilder
	}

	dbSession struct {
		*sqlxIO
		tx *lazyTx
		*info.Dialect
	}
)

func newDbIo(tx *lazyTx, dialect *info.Dialect) *dbSession {
	return &dbSession{
		sqlxIO:  newSqlxIO(),
		tx:      tx,
		Dialect: dialect,
	}
}

func New() *Executor {
	return &Executor{
		sqlBuilder: NewBuilder(),
	}
}

//Execute executes view dsql
func (e *Executor) Execute(ctx context.Context, aView *view.View, options ...Option) error {
	session, err := NewSession(view.NewSelectors(), aView)
	if err != nil {
		return err
	}
	if err := Options(options).Apply(session); err != nil {
		return err
	}
	return e.Exec(ctx, session)
}

//TODO: remove reflection
//TODO: customize global batch collector
func (e *Executor) Exec(ctx context.Context, session *Session) error {
	state, data, err := e.sqlBuilder.Build(session.View, session.Lookup(session.View))
	if state != nil {
		session.State = state
		defer session.State.Flush(expand.StatusFailure)
	}
	if err != nil {
		return err
	}

	if err = e.exec(ctx, session, data, state.DataUnit); err != nil {
		return err
	}

	return state.Flush(expand.StatusSuccess)
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

	dialect, err := session.View.Connector.Dialect(ctx)
	if err != nil {
		return err
	}
	aTx := newLazyTx(db, canBeBatchedGlobally)
	dbSession := newDbIo(aTx, dialect)

	for i := range data {
		if err = e.execData(ctx, dbSession, data[i], session, criteria, db, canBeBatchedGlobally); err != nil {
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

func (e *Executor) execData(ctx context.Context, dbSession *dbSession, data *SQLStatment, session *Session, criteria *expand.DataUnit, db *sql.DB, canBeBatchedGlobally bool) error {
	if strings.TrimSpace(data.SQL) == "" {
		return nil
	}
	if executable, ok := criteria.IsServiceExec(data.SQL); ok {
		switch executable.ExecType {
		case expand.ExecTypeInsert:
			return e.handleInsert(ctx, dbSession, session, executable, db, canBeBatchedGlobally)
		case expand.ExecTypeUpdate:
			return e.handleUpdate(ctx, dbSession, db, executable)
		default:
			return fmt.Errorf("unsupported exec type: %v\n", executable.ExecType)
		}
	}
	tx, err := dbSession.tx.Tx()
	if err != nil {
		return err
	}

	return e.executeStatement(ctx, tx, data, session)
}

func (e *Executor) handleUpdate(ctx context.Context, dbSession *dbSession, db *sql.DB, executable *expand.Executable) error {

	//service, err := update.New(ctx, db, executable.Table)
	service, err := dbSession.Updater(ctx, db, executable.Table, dbSession.Dialect)
	if err != nil {
		return err
	}

	options, err := dbSession.tx.PrepareTxOptions()
	if err != nil {
		return err
	}
	options = append(options, db)
	_, err = service.Exec(ctx, executable.Data, options...)
	return err
}

func (e *Executor) handleInsert(ctx context.Context, dbSession *dbSession, session *Session, executable *expand.Executable, db *sql.DB, canBeBatchedGlobally bool) error {

	canBeBatched := session.View.TableBatches[executable.Table] && e.dialectSupportsBatching(ctx, session.View)

	var options []option.Option
	options = append(options, dbSession.Dialect, db)

	//service, err := insert.New(ctx, db, executable.Table)
	service, err := dbSession.Inserter(ctx, db, executable.Table, options...)
	if err != nil {
		return err
	}

	if !canBeBatched {
		tx, err := dbSession.tx.Tx()
		if err != nil {
			return err
		}

		options = append(options, tx)
		_, _, err = service.Exec(ctx, executable.Data, options...)
		return err
	}

	collection := session.Collection(executable)
	collection.Append(executable.Data)
	if !executable.IsLast {
		return nil
	}

	if !canBeBatchedGlobally {
		options, err := dbSession.tx.PrepareTxOptions()
		if err != nil {
			return err
		}
		batchSize := 100
		if collection.Len() < batchSize {
			batchSize = collection.Len()
		}
		options = append(options, option.BatchSize(batchSize))
		_, _, err = service.Exec(ctx, collection.Unwrap(), append(options, dbSession.Dialect)...)
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
