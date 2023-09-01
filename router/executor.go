package router

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/executor/session"
	"github.com/viant/datly/template/expand"
	vsession "github.com/viant/datly/view/session"
	"github.com/viant/toolbox"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/async"
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

func NewExecutor(route *Route, request *http.Request, response http.ResponseWriter) *HandlerExecutor {
	return &HandlerExecutor{
		route:    route,
		request:  request,
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
		fmt.Printf("POPULATE ERR: %T %+v\n", err, err)
		return nil, err
	}
	toolbox.Dump(sessionState.State().Lookup(e.route.View).Template.State())
	sess, err := executor.NewSession(sessionState, e.route.View)
	if err != nil {
		return nil, err
	}
	if sess != nil {
		sess.DataUnit = e.dataUnit
	}
	e.session = sess
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

	sess := session.NewSession(
		session.WithTemplateFlush(func(ctx context.Context) error {
			return e.Execute(ctx)
		}),
		session.WithRedirect(e.redirect),
		session.WithStater(e.session.SessionState),
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

	newExecutor := NewExecutor(match, e.request, e.response)
	return newExecutor.SessionHandlerService(ctx)
}

func (e *HandlerExecutor) newAsync() async.Async {
	return &AsyncHandler{
		executor: e,
	}
}

func (e *HandlerExecutor) newHttp() http2.Http {
	return &Httper{
		executor: e,
	}
}
