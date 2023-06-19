package router

import (
	"context"
	"database/sql"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/executor/session"
	"github.com/viant/datly/template/expand"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/validator"
	"net/http"
)

type (
	Executor struct {
		executed bool
		session  *executor.Session
		route    *Route
		request  *http.Request
		params   *RequestParams
		dataUnit *expand.DataUnit
		tx       *sql.Tx
	}

	DBProvider struct {
		db *sql.DB
	}
)

func (d *DBProvider) Db() (*sql.DB, error) {
	return d.db, nil
}

func NewExecutor(route *Route, request *http.Request, params *RequestParams) *Executor {
	return &Executor{
		route:    route,
		request:  request,
		params:   params,
		dataUnit: expand.NewDataUnit(route.View),
	}
}

func (e *Executor) Session(ctx context.Context) (*executor.Session, error) {
	if e.session != nil {
		return e.session, nil
	}

	params, err := e.RequestParams(ctx)
	if err != nil {
		return nil, err
	}

	selectors, _, err := CreateSelectorsFromRoute(ctx, e.route, e.request, params, e.route.Index._viewDetails...)
	if err != nil {
		return nil, err
	}

	sess, err := executor.NewSession(selectors, e.route.View)
	if sess != nil {
		sess.DataUnit = e.dataUnit
	}

	e.session = sess
	return e.session, err
}

func (e *Executor) SessionHandler(ctx context.Context) (handler.Session, error) {
	params, err := e.RequestParams(ctx)
	if err != nil {
		return nil, err
	}

	return session.NewSession(
		session.WithTemplateFlush(func(ctx context.Context) error {
			return e.Execute(ctx)
		}),
		session.WithStater(e.route.NewStater(e.request, params)),
		session.WithSql(e.newSqlService),
	), nil
}

func (e *Executor) newValidator() *validator.Service {
	return validator.New(&Validator{
		validator: expand.CommonValidator(),
	})
}

func (e *Executor) RequestParams(ctx context.Context) (*RequestParams, error) {
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

func (e *Executor) newSqlService(options *sqlx.Options) (sqlx.Sqlx, error) {
	unit, err := e.getDataUnit(options)
	if err != nil {
		return nil, err
	}

	var txStartedNotifier func(tx *sql.Tx)
	if unit == e.dataUnit { //we are using View that can contain SQL Statements in Velty
		txStartedNotifier = e.txStarted
	}

	return &SqlxService{
		txNotifier: txStartedNotifier,
		dataUnit:   unit,
		options:    options,
		validator:  e.newValidator(),
		connectors: e.route._resource.GetConnectors(),
		params:     e.params,
	}, nil
}

func (e *Executor) getDataUnit(options *sqlx.Options) (*expand.DataUnit, error) {
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

func (e *Executor) Execute(ctx context.Context) error {
	if e.executed {
		return nil
	}
	e.executed = true

	sess, err := e.Session(ctx)
	if err != nil {
		return err
	}

	service := executor.New()

	var dbOptions []executor.DBOption
	if e.tx != nil {
		dbOptions = append(dbOptions, executor.WithTx(e.tx))
	}

	return service.Exec(ctx, sess, dbOptions...)
}

func (e *Executor) txStarted(tx *sql.Tx) {
	e.tx = tx
}
