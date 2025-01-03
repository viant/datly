package executor

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/logger"
	expand2 "github.com/viant/datly/service/executor/expand"
	vsession "github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/insert/batcher"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"reflect"
)

type (
	Executor struct {
		sqlBuilder *SqlBuilder
	}

	dbSession struct {
		*sqlxIO
		dialect *info.Dialect

		tx *lazyTx
		*dbSession
		canBeBatchedGlobally bool
		dbSource             DBSource
		collections          map[string]*batcher.Collection
		logger               *logger.Adapter
	}

	DBOption  func(options *DBOptions)
	DBOptions struct {
		tx     *sql.Tx
		logger *logger.Adapter
	}
)

func newDbIo(tx *lazyTx, dialect *info.Dialect, dbSource DBSource, canBeBatchedGlobally bool, logger *logger.Adapter) *dbSession {
	return &dbSession{
		sqlxIO:               newSqlxIO(),
		tx:                   tx,
		dialect:              dialect,
		dbSource:             dbSource,
		canBeBatchedGlobally: canBeBatchedGlobally,
		collections:          map[string]*batcher.Collection{},
		logger:               logger,
	}
}

func New() *Executor {
	return &Executor{
		sqlBuilder: NewBuilder(),
	}
}

// Execute executes view dql
func (e *Executor) Execute(ctx context.Context, aView *view.View, options ...Option) error {
	sessionState := vsession.New(aView)
	session, err := NewSession(sessionState, aView)
	if err != nil {
		return err
	}
	if err = Options(options).Apply(session); err != nil {
		return err
	}
	return e.Exec(ctx, session)
}

// TODO: remove reflection
// TODO: customize global batch collector
func (e *Executor) Exec(ctx context.Context, sess *Session, options ...DBOption) error {
	state, data, err := e.sqlBuilder.Build(sess.View, sess.Lookup(sess.View), sess.SessionHandler, sess.DataUnit)
	if state != nil {
		sess.TemplateState = state
		defer sess.TemplateState.Flush(expand2.StatusFailure)
	}

	if err != nil {
		return err
	}

	source := NewViewDBSource(sess.View)
	iterator := NewTemplateStmtIterator(state.DataUnit, data)

	options = append(options, WithLogger(sess.View.Logger))
	if err = e.ExecuteStmts(ctx, source, iterator, options...); err != nil {
		return err
	}

	return state.Flush(expand2.StatusSuccess)
}

func (e *Executor) ExecuteStmts(ctx context.Context, dbSource DBSource, it StmtIterator, options ...DBOption) error {
	if !it.HasAny() {
		return nil
	}

	ops := &DBOptions{}
	for _, apply := range options {
		apply(ops)
	}

	canBeBatchedGlobally := dbSource.CanBatchGlobally()
	db, err := dbSource.Db(ctx)
	if err != nil {
		return err
	}

	dialect, err := dbSource.Dialect(ctx)
	if err != nil {
		return err
	}

	aTx := newLazyTx(db, canBeBatchedGlobally, ops.tx)
	aDbSession := newDbIo(aTx, dialect, dbSource, canBeBatchedGlobally, ops.logger)

	for it.HasNext() {
		next := it.Next()
		if err = e.execData(ctx, aDbSession, next, db); err != nil {
			_ = aTx.RollbackIfNeeded()
			return err
		}
	}

	return aTx.CommitIfNeeded()
}

func (e *Executor) execData(ctx context.Context, sess *dbSession, data interface{}, db *sql.DB) error {
	switch actual := data.(type) {
	case *expand2.Executable:
		if actual.Executed() {
			return nil
		}

		actual.MarkAsExecuted()
		switch actual.ExecType {
		case expand2.ExecTypeInsert:
			return e.handleInsert(ctx, sess, actual, db)
		case expand2.ExecTypeUpdate:
			return e.handleUpdate(ctx, sess, db, actual)
		default:
			return fmt.Errorf("unsupported exec type: %v\n", actual.ExecType)
		}

	case *expand2.SQLStatment:
		if len(actual.SQL) == 0 {
			return nil
		}

		tx, err := sess.tx.Tx()
		if err != nil {
			return err
		}

		return e.executeStatement(ctx, tx, actual, sess)
	}

	return fmt.Errorf("unsupported query type %T", data)
}

func (e *Executor) handleUpdate(ctx context.Context, sess *dbSession, db *sql.DB, executable *expand2.Executable) error {
	service, err := sess.Updater(ctx, db, executable.Table, e.dbOptions(db, sess))
	if err != nil {
		return err
	}

	options, err := sess.tx.PrepareTxOptions()
	if err != nil {
		return err
	}
	options = append(options, db)
	_, err = service.Exec(ctx, executable.Data, options...)
	return err
}

func (e *Executor) handleInsert(ctx context.Context, sess *dbSession, executable *expand2.Executable, db *sql.DB) error {
	canBeBatched := sess.supportLocalBatch()
	//TODO remove this option make no sense unless its blacklist -&& sess.dbSource.CanBatch(executable.Table)
	options := e.dbOptions(db, sess)
	service, err := sess.Inserter(ctx, db, executable.Table, options...)
	if err != nil {
		return err
	}

	if !canBeBatched {
		tx, err := sess.tx.Tx()
		if err != nil {
			return err
		}

		options = append(options, tx)
		_, _, err = service.Exec(ctx, executable.Data, options...)
		return err
	}

	if !executable.IsLast {
		return nil
	}

	if !sess.canBeBatchedGlobally {
		options, err := sess.tx.PrepareTxOptions()
		if err != nil {
			return err
		}
		batchSize := 100
		rType := reflect.TypeOf(executable.Data)
		if rType.Kind() == reflect.Slice {
			actual := reflect.ValueOf(executable.Data)
			if actual.Len() < batchSize {
				batchSize = actual.Len()
			}
		}
		options = append(options, option.BatchSize(batchSize))
		options = append(options, e.dbOptions(db, sess))
		_, _, err = service.Exec(ctx, executable.Data, options...)
		return err
	}

	//TODO: !!!!!! :^^^^^^^^:
	aBatcher, err := batcherRegistry.GetBatcher(executable.Table, reflect.TypeOf(executable.Data), db, &batcher.Config{
		MaxElements:   100,
		MaxDurationMs: 10,
		BatchSize:     100,
	})

	if err != nil {
		return err
	}

	//TODO: remove reflection
	rSlice := reflect.ValueOf(executable.Data)
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

func (e *Executor) dbOptions(db *sql.DB, sess *dbSession) []option.Option {
	options := []option.Option{db}
	if sess.dialect != nil {
		options = append(options, sess.dialect)
	}
	return options
}

func (e *Executor) dialectSupportsBatching(ctx context.Context, aView *view.View) bool {
	dialect, err := aView.Connector.Dialect(ctx)
	return err == nil && dialect.Insert.MultiValues()
}

func (e *Executor) executeStatement(ctx context.Context, tx *sql.Tx, stmt *expand2.SQLStatment, sess *dbSession) error {
	_, err := tx.ExecContext(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		if sess.logger != nil {
			sess.logger.LogDatabaseErr(stmt.SQL, err, stmt.Args...)
		}

		err = fmt.Errorf("error occured while connecting to database")
	}

	return err
}

func (s *dbSession) collection(executable *expand2.Executable) *batcher.Collection {
	if collection, ok := s.collections[executable.Table]; ok {
		return collection
	}

	collection := batcher.NewCollection(reflect.TypeOf(executable.Data))
	s.collections[executable.Table] = collection
	return collection
}

func (s *dbSession) supportLocalBatch() bool {
	return s.dialect != nil && s.dialect.Insert.MultiValues()
}

func WithTx(tx *sql.Tx) DBOption {
	return func(options *DBOptions) {
		options.tx = tx
	}
}

func WithLogger(log *logger.Adapter) DBOption {
	return func(options *DBOptions) {
		options.logger = log
	}
}
