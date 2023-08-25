package router

import (
	"context"
	"database/sql"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/executor/session"
	"github.com/viant/datly/template/expand"
	vsession "github.com/viant/datly/view/session"
	"github.com/viant/xdatly/handler"
	async2 "github.com/viant/xdatly/handler/async"
	http2 "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/validator"
	"net/http"
)

type (
	HandlerExecutor struct {
		executed       bool
		session        *executor.Session
		route          *Route
		request        *http.Request
		params         *RequestParams
		dataUnit       *expand.DataUnit
		tx             *sql.Tx
		sessionHandler *session.Session
		response       http.ResponseWriter
	}

	DBProvider struct {
		db *sql.DB
	}
)

func (d *DBProvider) Db() (*sql.DB, error) {
	return d.db, nil
}

func NewExecutor(route *Route, request *http.Request, params *RequestParams, response http.ResponseWriter) *HandlerExecutor {
	return &HandlerExecutor{
		route:    route,
		request:  request,
		params:   params,
		dataUnit: expand.NewDataUnit(route.View),
		response: response,
	}
}

func (e *HandlerExecutor) Session(ctx context.Context) (*executor.Session, error) {
	if e.session != nil {
		return e.session, nil
	}
	sessionState := vsession.New(e.route.View, vsession.WithLocatorOptions(e.route.LocatorOptions(e.request)...))
	if err := sessionState.Populate(ctx); err != nil {
		return nil, err
	}
	sess, err := executor.NewSession(sessionState.ResourceState(), e.route.View)
	if err != nil {
		return nil, err
	}
	if sess != nil {
		sess.DataUnit = e.dataUnit
	}

	sessionHandler, err := e.SessionHandlerService(ctx)
	if err != nil {
		return nil, err
	}

	e.session = sess
	sess.SessionHandler = sessionHandler
	return e.session, err
}

func (e *HandlerExecutor) SessionHandler(ctx context.Context) (handler.Session, error) {
	service, err := e.SessionHandlerService(ctx)
	if err != nil {
		return nil, err
	}

	if service == nil {
		return nil, err
	}

	return service, err
}

func (e *HandlerExecutor) SessionHandlerService(ctx context.Context) (*session.Session, error) {
	if e.sessionHandler != nil {
		return e.sessionHandler, nil
	}

	params, err := e.RequestParams(ctx)
	if err != nil {
		return nil, err
	}

	sess := session.NewSession(
		session.WithTemplateFlush(func(ctx context.Context) error {
			return e.Execute(ctx)
		}),
		session.WithRedirect(e.redirect),
		session.WithStater(e.route.NewStater(e.request, params)),
		session.WithSql(e.newSqlService),
		session.WithAsync(e.newAsync),
		session.WithHttp(e.newHttp),
	)

	e.sessionHandler = sess
	return sess, nil
}

func (e *HandlerExecutor) newValidator() *validator.Service {
	return validator.New(&Validator{
		validator: expand.CommonValidator(),
	})
}

func (e *HandlerExecutor) RequestParams(ctx context.Context) (*RequestParams, error) {
	if e.params != nil {
		return e.params, nil
	}

	parameters, err := NewRequestParameters(e.request, e.route)
	if err != nil {
		return nil, err
	}

	e.params = parameters
	return e.params, nil
}

func (e *HandlerExecutor) newSqlService(options *sqlx.Options) (sqlx.Sqlx, error) {
	unit, err := e.getDataUnit(options)
	if err != nil {
		return nil, err
	}

	var txStartedNotifier func(tx *sql.Tx)
	if unit == e.dataUnit { //we are using View that can contain SQL Statements in Velty
		txStartedNotifier = e.txStarted
	}

	return &SqlxService{
		txNotifier:    txStartedNotifier,
		dataUnit:      unit,
		options:       options,
		validator:     e.newValidator(),
		connectors:    e.route._resource.GetConnectors(),
		params:        e.params,
		mainConnector: e.route.View.Connector,
	}, nil
}

func (e *HandlerExecutor) getDataUnit(options *sqlx.Options) (*expand.DataUnit, error) {
	if (options.WithDb == nil && options.WithTx == nil) || options.WithConnector == e.route.View.Connector.Name {
		return e.dataUnit, nil
	}

	if options.WithDb != nil {
		return expand.NewDataUnit(&DBProvider{db: options.WithDb}), nil
	}

	if options.WithConnector != "" {
		connector, err := e.route._resource.Connector(options.WithConnector)
		if err != nil {
			return nil, err
		}

		db, err := connector.DB()
		if err != nil {
			return nil, err
		}

		return expand.NewDataUnit(&DBProvider{db: db}), nil
	}

	return e.dataUnit, nil
}

func (e *HandlerExecutor) Execute(ctx context.Context) error {
	if e.executed {
		return nil
	}

	e.executed = true
	service := executor.New()

	var dbOptions []executor.DBOption
	if e.tx != nil {
		dbOptions = append(dbOptions, executor.WithTx(e.tx))
	}

	return service.ExecuteStmts(ctx, executor.NewViewDBSource(e.route.View), &SqlxIterator{toExecute: e.dataUnit.Statements.Executable}, dbOptions...)
}

func (e *HandlerExecutor) ExpandAndExecute(ctx context.Context) (*executor.Session, error) {
	sess, err := e.Session(ctx)
	if err != nil {
		return nil, err
	}

	service := executor.New()

	var dbOptions []executor.DBOption
	if e.tx != nil {
		dbOptions = append(dbOptions, executor.WithTx(e.tx))
	}

	return sess, service.Exec(ctx, sess, dbOptions...)
}

func (e *HandlerExecutor) txStarted(tx *sql.Tx) {
	e.tx = tx
}

func (e *HandlerExecutor) redirect(ctx context.Context, route *http2.Route) (handler.Session, error) {
	match, err := e.route.match(ctx, route)
	if err != nil {
		return nil, err
	}

	newExecutor := NewExecutor(match, e.request, nil, e.response)
	return newExecutor.SessionHandlerService(ctx)
}

func (e *HandlerExecutor) newAsync() async2.Async {
	return &AsyncHandler{
		executor: e,
	}
}

func (e *HandlerExecutor) newHttp() http2.Http {
	return &Httper{
		executor: e,
	}
}
