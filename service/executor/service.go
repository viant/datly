package executor

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/viant/datly/logger"
	expand2 "github.com/viant/datly/service/executor/expand"
	vsession "github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/insert/batcher"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"github.com/viant/xdatly/handler/exec"
	"github.com/viant/xdatly/handler/response"
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
		dbSource    DBSource
		collections map[string]*batcher.Collection
		logger      *logger.Adapter
		inserted    int32
		updated     int32
	}

	DBOption  func(options *DBOptions)
	DBOptions struct {
		tx     *sql.Tx
		logger *logger.Adapter
	}
)

func newDbIo(tx *lazyTx, dialect *info.Dialect, dbSource DBSource, logger *logger.Adapter) *dbSession {
	return &dbSession{
		sqlxIO:      newSqlxIO(),
		tx:          tx,
		dialect:     dialect,
		dbSource:    dbSource,
		collections: map[string]*batcher.Collection{},
		logger:      logger,
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
	state, data, err := e.sqlBuilder.Build(ctx, sess.View, sess.Lookup(sess.View), sess.SessionHandler, sess.DataUnit)
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
	db, err := dbSource.Db(ctx)
	if err != nil {
		return err
	}

	dialect, err := dbSource.Dialect(ctx)
	if err != nil {
		return err
	}

	aTx := newLazyTx(db, ops.tx)
	aDbSession := newDbIo(aTx, dialect, dbSource, ops.logger)

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
		case expand2.ExecTypeDelete:
			return e.handleDelete(ctx, sess, db, actual)
		default:
			return fmt.Errorf("unsupported '%v' db operation\n", actual.ExecType.String())
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

func (e *Executor) handleDelete(ctx context.Context, sess *dbSession, db *sql.DB, executable *expand2.Executable) error {
	now := time.Now()
	service, err := sess.Deleter(ctx, db, executable.Table, e.dbOptions(db, sess))
	if err != nil {
		return err
	}
	options, err := sess.tx.PrepareTxOptions()
	if err != nil {
		return err
	}
	options = append(options, db)

	deleted, err := service.Exec(ctx, executable.Data, options...)
	e.logMetrics(ctx, executable.Table, "DELETE", deleted, now, err)
	return err
}

func (e *Executor) handleUpdate(ctx context.Context, sess *dbSession, db *sql.DB, executable *expand2.Executable) error {
	now := time.Now()
	service, err := sess.Updater(ctx, db, executable.Table, e.dbOptions(db, sess))
	if err != nil {
		return err
	}
	options, err := sess.tx.PrepareTxOptions()
	if err != nil {
		return err
	}
	options = append(options, db)

	updated, err := service.Exec(ctx, executable.Data, options...)
	if err == nil {
		atomic.AddInt32(&sess.updated, int32(updated))
	}
	e.logMetrics(ctx, executable.Table, "UPDATE", updated, now, err)
	return err
}

func (e *Executor) logMetrics(ctx context.Context, table string, operation string, count int64, startTime time.Time, err error) {
	value := ctx.Value(exec.ContextKey)
	if value == nil {
		return
	}
	elapsed := time.Since(startTime)
	metric := response.Metric{
		View:      table,
		StartTime: startTime,
		EndTime:   time.Now(),
		Type:      operation,
		Rows:      int(count),
		ElapsedMs: int(elapsed.Milliseconds()),
		Elapsed:   elapsed.String(),
	}
	if err != nil {
		metric.Error = err.Error()
	}
	value.(*exec.Context).AppendMetrics(&metric)
}

func (e *Executor) handleInsert(ctx context.Context, sess *dbSession, executable *expand2.Executable, db *sql.DB) error {
	started := time.Now()
	batchable := sess.supportLocalBatch()
	//TODO remove this option make no sense unless its blacklist -&& sess.dbSource.CanBatch(executable.Table)
	options := e.dbOptions(db, sess)
	service, err := sess.Inserter(ctx, db, executable.Table, options...)
	if err != nil {
		return err
	}

	var inserted = int64(0)
	if !batchable {
		tx, err := sess.tx.Tx()
		if err != nil {
			return err
		}
		options = append(options, tx)
		inserted, _, err = service.Exec(ctx, executable.Data, options...)
		if err == nil {
			atomic.AddInt32(&sess.inserted, int32(inserted))
		}
		e.logMetrics(ctx, executable.Table, "INSERT", inserted, started, err)
		return err
	}

	options, err = sess.tx.PrepareTxOptions()
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
	inserted, _, err = service.Exec(ctx, executable.Data, options...)
	if err == nil {
		atomic.AddInt32(&sess.inserted, int32(inserted))
	}
	isInvalidConnection := err != nil && strings.Contains(err.Error(), "invalid connection")
	if isInvalidConnection && atomic.LoadInt32(&sess.inserted) == 0 && atomic.LoadInt32(&sess.updated) == 0 {
		var dErr error
		db, dErr = sess.dbSource.Db(ctx)
		if dErr != nil {
			return fmt.Errorf("failed after retry: %w", err)
		}
		sess.tx.db = db
		sess.tx.tx = nil
		if _, err = sess.tx.Tx(); err != nil {
			return err
		}
		inserted, _, err = service.Exec(ctx, executable.Data, options...)
	}
	e.logMetrics(ctx, executable.Table, "INSERT", inserted, started, err)
	return err
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
