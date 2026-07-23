package handler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	executor "github.com/viant/datly/service/executor"
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/executor/extension"
	"github.com/viant/datly/service/executor/uow"
	session "github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/xdatly/handler"
	hauth "github.com/viant/xdatly/handler/auth"
	http2 "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/sqlx"
	hstate "github.com/viant/xdatly/handler/state"
	"github.com/viant/xdatly/handler/validator"
)

type (
	Executor struct {
		executed        bool
		session         *session.Session
		executorSession *executor.Session
		handlerSession  *extension.Session
		*options
		component  *repository.Component
		view       *view.View
		connectors view.Connectors
		dataUnit   *expand.DataUnit
		dataUnits  map[string]*expand.DataUnit
		unitsByDB  map[*sql.DB]*expand.DataUnit
		unitTx     map[*expand.DataUnit]*sql.Tx
		unitMu     sync.Mutex
		buffers    map[*expand.DataUnit]*uow.Buffer
		scope      *uow.Scope
		frame      *uow.Frame
		bufferErr  error
		bufferMu   sync.Mutex
		ctx        context.Context
		tx         *sql.Tx
		txOwned    bool
		response   http.ResponseWriter
	}

	DBProvider struct {
		db *sql.DB
	}

	DbSource struct {
		db      *sql.DB
		dialect *info.Dialect
	}
)

func (d *DBProvider) Db() (*sql.DB, error) {
	return d.db, nil
}

func (d *DbSource) Db(_ context.Context) (*sql.DB, error) {
	return d.db, nil
}

func (d *DbSource) Dialect(ctx context.Context) (*info.Dialect, error) {
	if d.dialect != nil {
		return d.dialect, nil
	}
	var err error
	d.dialect, err = getDialect(ctx, d.db)
	return d.dialect, err
}

func NewExecutor(aView *view.View, aSession *session.Session, opts ...Option) *Executor {
	return &Executor{
		view:     aView,
		options:  newOptions(opts...),
		session:  aSession,
		dataUnit: expand.NewDataUnit(aView),
	}
}

func (e *Executor) Session(ctx context.Context) (*executor.Session, error) {
	if err := e.ensureUnitOfWork(ctx); err != nil {
		return nil, err
	}
	if e.executorSession != nil {
		return e.executorSession, nil
	}
	sess, err := executor.NewSession(e.session, e.view)
	if err != nil {
		return nil, err
	}
	if sess != nil {
		sess.DataUnit = e.dataUnit
	}
	e.executorSession = sess
	sessionHandler, err := e.HandlerSession(ctx)
	if err != nil {
		return nil, err
	}

	e.executorSession = sess
	sess.SessionHandler = sessionHandler
	// inherit tx from session options if available
	if e.tx == nil {
		if tx := e.session.Options.SqlTx(); tx != nil {
			e.tx = tx
		}
	}
	return e.executorSession, err
}

func (e *Executor) NewHandlerSession(ctx context.Context, opts ...Option) (handler.Session, error) {
	aSession, err := e.HandlerSession(ctx, opts...)
	if err != nil {
		return nil, err
	}

	if aSession == nil {
		return nil, err
	}

	return aSession, err
}

func (e *Executor) HandlerSession(ctx context.Context, opts ...Option) (*extension.Session, error) {
	if err := e.ensureUnitOfWork(ctx); err != nil {
		return nil, err
	}
	if e.handlerSession != nil {
		return e.handlerSession, nil
	}
	sess := e.newSession(e.session, opts...)
	e.handlerSession = sess
	return sess, nil
}

func (e *Executor) newSession(aSession *session.Session, opts ...Option) *extension.Session {
	var options = e.options.Clone(opts)
	e.session.Apply(session.WithTypes(options.Types...))
	e.session.Apply(session.WithEmbeddedFS(options.embedFS))
	if options.auth != nil {
		e.auth = options.auth
	}
	if e.logger == nil {
		e.logger = options.logger
	}
	res := e.view.GetResource()
	sess := extension.NewSession(
		extension.WithTemplateFlush(func(ctx context.Context) error {
			return e.flushTemplate(ctx)
		}),
		extension.WithStater(&sessionInjector{executor: e, session: aSession}),
		extension.WithRedirect(e.redirect),
		extension.WithSql(e.newSqlService),
		extension.WithTransaction(e.transaction),
		extension.WithHttp(e.newHttp),
		extension.WithLogger(e.logger),
		extension.WithAuth(e.newAuth),
		extension.WithMessageBus(res.MessageBuses),
	)
	return sess
}

// sessionInjector restores the invocation context captured by the handler
// session before delegating state operations. Public handler APIs accept a
// context supplied by the caller, which can still identify the parent frame;
// component binding must run in the child frame owned by this session.
type sessionInjector struct {
	executor *Executor
	session  *session.Session
}

func (i *sessionInjector) context(ctx context.Context) context.Context {
	if i.executor != nil {
		ctx = i.executor.invocationContext(ctx)
	}
	if i.session != nil {
		ctx = i.session.Context(ctx, true)
	}
	return ctx
}

// invocationContext keeps public handler facades on the component frame that
// created them. Legacy handlers commonly pass their outer request context to a
// child session's Stater or Http facade; that context must not move the child
// executor back to the parent frame.
func (e *Executor) invocationContext(ctx context.Context) context.Context {
	if e == nil {
		return ctx
	}
	return uow.Propagate(e.ctx, ctx)
}

func (i *sessionInjector) Into(ctx context.Context, value interface{}, opts ...hstate.Option) error {
	return i.session.Into(i.context(ctx), value, opts...)
}

func (i *sessionInjector) Bind(ctx context.Context, value interface{}, opts ...hstate.Option) error {
	return i.session.Bind(i.context(ctx), value, opts...)
}

func (i *sessionInjector) Value(ctx context.Context, key string) (interface{}, bool, error) {
	return i.session.Value(i.context(ctx), key)
}

func (i *sessionInjector) ValuesOf(ctx context.Context, value interface{}) (map[string]interface{}, error) {
	return i.session.ValuesOf(i.context(ctx), value)
}

func (e *Executor) transaction(ctx context.Context) (*sql.Tx, error) {
	e.unitMu.Lock()
	buffer := e.buffers[e.dataUnit]
	e.unitMu.Unlock()
	if buffer == nil {
		return nil, fmt.Errorf("invocation transaction is unavailable")
	}
	return buffer.Transaction(ctx)
}

func (e *Executor) newValidator() *validator.Service {
	return validator.New(&Validator{
		validator: expand.CommonValidator(),
	})
}

func (e *Executor) newSqlService(options *sqlx.Options) (sqlx.Sqlx, error) {
	unit, err := e.getDataUnit(options)
	if err != nil {
		return nil, err
	}

	var txStartedNotifier func(tx *sql.Tx)
	if unit == e.dataUnit { //we are using View that can contain SQL Statements in Velty
		txStartedNotifier = e.txStarted
	}
	// default SQLx tx to executor tx to avoid internal Begin/Commit if caller provided one
	if options.WithTx == nil && e.tx != nil {
		options.WithTx = e.tx
	}
	if e.scope != nil && options.WithTx != nil {
		db, dbErr := unit.MetaSource.Db()
		if dbErr != nil {
			return nil, dbErr
		}
		if dbErr = e.scope.AdoptTransaction(db, options.WithTx); dbErr != nil {
			return nil, dbErr
		}
	}
	e.unitMu.Lock()
	if e.unitTx == nil {
		e.unitTx = map[*expand.DataUnit]*sql.Tx{}
	}
	if options.WithTx != nil {
		e.unitTx[unit] = options.WithTx
	}
	buffer := e.buffers[unit]
	e.unitMu.Unlock()
	return &Service{
		txNotifier:    txStartedNotifier,
		dataUnit:      unit,
		options:       options,
		validator:     e.newValidator(),
		connectors:    e.connectors,
		mainConnector: e.view.Connector,
		buffer:        buffer,
		tx:            options.WithTx,
	}, nil
}

func (e *Executor) getDataUnit(options *sqlx.Options) (*expand.DataUnit, error) {
	e.unitMu.Lock()
	defer e.unitMu.Unlock()
	e.ensureConnectors()
	if (options.WithDb == nil && options.WithTx == nil) && options.WithConnector == e.view.Connector.Name {
		return e.dataUnit, nil
	}

	if options.WithDb != nil {
		if unit := e.unitsByDB[options.WithDb]; unit != nil {
			return unit, nil
		}
		unit := expand.NewDataUnit(&DBProvider{db: options.WithDb})
		if e.unitsByDB == nil {
			e.unitsByDB = map[*sql.DB]*expand.DataUnit{}
		}
		e.unitsByDB[options.WithDb] = unit
		if err := e.attachBuffer(unit, func(context.Context) (*sql.DB, error) { return options.WithDb, nil }, options.WithTx); err != nil {
			return nil, err
		}
		return unit, nil
	}

	if options.WithConnector != "" {
		if e.dataUnits == nil {
			e.dataUnits = make(map[string]*expand.DataUnit)
		}
		if ret, ok := e.dataUnits[options.WithConnector]; ok {
			return ret, nil
		}
		var connector *view.Connector
		if len(e.connectors) == 0 {
			connector, _ = e.view.GetResource().Connector(options.WithConnector)
		} else {
			connector, _ = e.connectors.Lookup(options.WithConnector)
		}
		if connector == nil {
			return nil, fmt.Errorf("failed to lookup connector %v", options.WithConnector)
		}

		if _, ok := e.connectors[options.WithConnector]; !ok {
			e.connectors[options.WithConnector] = connector
		}

		db, err := connector.DB()
		if err != nil {
			return nil, err
		}

		unit := expand.NewDataUnit(&DBProvider{db: db})

		e.dataUnits[options.WithConnector] = unit
		if err := e.attachBuffer(unit, func(context.Context) (*sql.DB, error) { return db, nil }, options.WithTx); err != nil {
			return nil, err
		}
		return unit, nil
	}

	return e.dataUnit, nil
}

func (e *Executor) ensureConnectors() {
	if len(e.connectors) == 0 {
		e.connectors = make(view.Connectors)
		if res := e.view.GetResource(); res != nil {
			for _, connector := range res.Connectors {
				e.connectors[connector.Name] = connector
			}
		}
	}
}

func (e *Executor) Execute(ctx context.Context) error {
	if e.executed {
		return nil
	}
	e.executed = true
	if e.scope != nil {
		return e.getBufferErr()
	}
	service := executor.New()
	var dbOptions []executor.DBOption
	if e.tx != nil {
		dbOptions = append(dbOptions, executor.WithTx(e.tx))
	}

	err := service.ExecuteStmts(ctx, executor.NewViewDBSource(e.view), newSqlxIterator(e.dataUnit.Statements.Snapshot()), dbOptions...)
	if err != nil {
		return e.completeOwnedTx(err)
	}
	e.unitMu.Lock()
	seen := map[*expand.DataUnit]bool{e.dataUnit: true}
	units := make([]*expand.DataUnit, 0, len(e.dataUnits)+len(e.unitsByDB))
	for _, unit := range e.dataUnits {
		if !seen[unit] {
			seen[unit] = true
			units = append(units, unit)
		}
	}
	for _, unit := range e.unitsByDB {
		if !seen[unit] {
			seen[unit] = true
			units = append(units, unit)
		}
	}
	unitTx := make(map[*expand.DataUnit]*sql.Tx, len(e.unitTx))
	for unit, tx := range e.unitTx {
		unitTx[unit] = tx
	}
	e.unitMu.Unlock()
	for _, unit := range units {
		dbSource := &DbSource{}
		dbSource.db, _ = unit.MetaSource.Db()
		unitOptions := []executor.DBOption(nil)
		if tx := unitTx[unit]; tx != nil {
			unitOptions = append(unitOptions, executor.WithTx(tx))
		}
		if err := service.ExecuteStmts(ctx, dbSource, newSqlxIterator(unit.Statements.Snapshot()), unitOptions...); err != nil {
			return e.completeOwnedTx(err)
		}
	}

	return e.completeOwnedTx(err)
}

func (e *Executor) flushTemplate(ctx context.Context) error {
	if e.scope == nil {
		return e.Execute(ctx)
	}
	if err := e.getBufferErr(); err != nil {
		return err
	}
	if buffer := e.bufferFor(e.dataUnit); buffer != nil {
		return buffer.Flush(ctx, "")
	}
	return nil
}

func (e *Executor) ExpandAndExecute(ctx context.Context) (*executor.Session, error) {
	sess, err := e.Session(ctx)
	if err != nil {
		return nil, err
	}
	service := executor.New()
	if e.scope != nil {
		ordered, buildErr := service.BuildBuffered(ctx, sess)
		if buildErr != nil {
			return nil, buildErr
		}
		if buffer := e.bufferFor(e.dataUnit); buffer != nil {
			if buildErr = buffer.Reconcile(ordered); buildErr != nil {
				return nil, buildErr
			}
		}
		return sess, e.getBufferErr()
	}

	var dbOptions []executor.DBOption
	if e.tx != nil {
		dbOptions = append(dbOptions, executor.WithTx(e.tx))
	}
	return sess, service.Exec(ctx, sess, dbOptions...)
}

func (e *Executor) bufferFor(unit *expand.DataUnit) *uow.Buffer {
	e.unitMu.Lock()
	defer e.unitMu.Unlock()
	return e.buffers[unit]
}

func (e *Executor) ensureUnitOfWork(ctx context.Context) error {
	scope, frame, ok := uow.FromContext(ctx)
	if !ok {
		// Backward-compatible callers may invoke a returned handler session with
		// an unscoped context. Once this executor belongs to a unit of work, keep
		// its captured invocation context so subsequent nested dispatches remain
		// children of this component rather than falling back to an ancestor.
		if e.scope != nil {
			return e.getBufferErr()
		}
		e.ctx = ctx
		return nil
	}
	if e.scope != nil {
		if e.scope != scope || e.frame != frame {
			viewName := ""
			if e.view != nil {
				viewName = e.view.Name
			}
			return fmt.Errorf("executor mutation scope mismatch: view=%s executor scope=%p frame=%s, context scope=%p frame=%s", viewName, e.scope, e.frame.DebugLabel(), scope, frame.DebugLabel())
		}
		e.ctx = ctx
		return e.getBufferErr()
	}
	e.ctx = ctx
	e.scope, e.frame = scope, frame
	if e.tx == nil && e.session != nil {
		e.tx = e.session.Options.SqlTx()
	}
	e.unitMu.Lock()
	defer e.unitMu.Unlock()
	if e.buffers == nil {
		e.buffers = map[*expand.DataUnit]*uow.Buffer{}
	}
	if e.unitsByDB == nil {
		e.unitsByDB = map[*sql.DB]*expand.DataUnit{}
	}
	return e.attachBuffer(e.dataUnit, func(context.Context) (*sql.DB, error) {
		if e.view == nil || e.view.Connector == nil {
			return nil, fmt.Errorf("view connector is required")
		}
		return e.view.Connector.DB()
	}, e.tx)
}

func (e *Executor) attachBuffer(unit *expand.DataUnit, resolve func(context.Context) (*sql.DB, error), tx *sql.Tx) error {
	if e.scope == nil || e.frame == nil || unit == nil {
		return nil
	}
	if e.buffers[unit] != nil {
		return nil
	}
	if tx != nil {
		db, err := resolve(e.ctx)
		if err != nil {
			return err
		}
		if err = e.scope.AdoptTransaction(db, tx); err != nil {
			return err
		}
	}
	buffer := e.frame.NewBuffer(resolve, tx, func(ctx context.Context, transaction *sql.Tx, value any) error {
		db, err := resolve(ctx)
		if err != nil {
			return err
		}
		source := &DbSource{db: db}
		return executor.New().ExecuteStmts(ctx, source, &sqlxIterator{toExecute: []interface{}{value}}, executor.WithTx(transaction))
	})
	buffer.SetBatchExecutor(func(ctx context.Context, transaction *sql.Tx, values []any) error {
		db, err := resolve(ctx)
		if err != nil {
			return err
		}
		return executor.New().ExecuteStmts(ctx, &DbSource{db: db}, newSqlxIterator(values), executor.WithTx(transaction))
	})
	e.buffers[unit] = buffer
	unit.SetTransactionRunner(e.ctx, func(fn func(*sql.Tx) error) error {
		return buffer.UseTransaction(e.ctx, fn)
	})
	unit.Statements.SetAppendObserver(func(value interface{}) {
		if err := buffer.Append(value); err != nil {
			e.setBufferErr(err)
		}
	})
	for _, value := range unit.Statements.Snapshot() {
		if err := buffer.Append(value); err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) setBufferErr(err error) {
	if err == nil {
		return
	}
	e.bufferMu.Lock()
	if e.bufferErr == nil {
		e.bufferErr = err
	}
	e.bufferMu.Unlock()
}

func (e *Executor) getBufferErr() error {
	e.bufferMu.Lock()
	defer e.bufferMu.Unlock()
	return e.bufferErr
}

func (e *Executor) txStarted(tx *sql.Tx) {
	e.tx = tx
	e.txOwned = tx != nil
}

func (e *Executor) completeOwnedTx(cause error) error {
	if !e.txOwned || e.tx == nil {
		return cause
	}
	tx := e.tx
	e.tx, e.txOwned = nil, false
	if cause != nil {
		return errors.Join(cause, tx.Rollback())
	}
	return tx.Commit()
}

func (e *Executor) redirect(ctx context.Context, route *http2.Route, opts ...hstate.Option) (handler.Session, error) {
	ctx = uow.Propagate(e.ctx, ctx)
	registry := e.session.Registry()
	if registry == nil {
		return nil, fmt.Errorf("registry was empty")
	}
	aComponent, err := registry.Lookup(ctx, contract.NewPath(route.Method, route.URL))
	if err != nil {
		return nil, err
	}
	ctx = uow.PrepareChild(ctx, uow.RelationImperative, "")
	if _, _, scoped := uow.FromContext(ctx); scoped {
		ctx, _, _, _, err = uow.Enter(ctx, route.Method+" "+route.URL)
		if err != nil {
			return nil, err
		}
	}
	originalRequest, _ := e.session.HttpRequest(ctx, e.session.Clone())

	request, _ := http.NewRequest(route.Method, route.URL, nil)
	if originalRequest != nil {
		request.Header = originalRequest.Header
	}
	stateOptions := hstate.NewOptions(opts...)
	unmarshal := aComponent.UnmarshalFunc(request)
	locatorOptions := aComponent.LocatorOptions(request, hstate.NewForm(), unmarshal)
	if stateOptions.Query() != nil {
		locatorOptions = append(locatorOptions, locator.WithQuery(stateOptions.Query()))
	}
	if stateOptions.Form() != nil {
		locatorOptions = append(locatorOptions, locator.WithForm(stateOptions.Form()))
	}
	if stateOptions.Headers() != nil {
		locatorOptions = append(locatorOptions, locator.WithHeaders(stateOptions.Headers()))
	}
	if stateOptions.PathParameters() != nil {
		locatorOptions = append(locatorOptions, locator.WithPathParameters(stateOptions.PathParameters()))
	}
	if stateOptions.HttpRequest() != nil {
		locatorOptions = append(locatorOptions, locator.WithRequest(stateOptions.HttpRequest()))
	}
	aSession := session.New(aComponent.View,
		session.WithAuth(e.auth),
		session.WithLocatorOptions(locatorOptions...),
		session.WithOperate(e.session.Options.Operate()),
		session.WithTypes(&aComponent.Contract.Input.Type, &aComponent.Contract.Output.Type),
		session.WithComponent(aComponent),
		session.WithLogger(e.logger),
		session.WithRegistry(registry),
	)
	if tx := stateOptions.SqlTx(); tx != nil {
		// associate tx with session; child executor will reuse it
		aSession.Apply(session.WithSQLTx(tx))
	}

	err = aSession.InitKinds(state.KindComponent, state.KindHeader, state.KindRequestBody, state.KindForm, state.KindQuery)
	if err != nil {
		return nil, err
	}
	ctx = aSession.Context(ctx, true)
	anExecutor := NewExecutor(aComponent.View, aSession)
	// ensure Execute(ctx) uses the provided tx (avoid autocommit)
	if tx := stateOptions.SqlTx(); tx != nil {
		anExecutor.tx = tx
	}
	return anExecutor.NewHandlerSession(ctx, WithLogger(aSession.Logger()))
}

func (e *Executor) newHttp() http2.Http {
	return NewHttp(e, e.view.GetResource())
}

func (e *Executor) newAuth() hauth.Auth {
	return NewAuth(e.auth)
}
