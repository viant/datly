package handler

import (
	"context"
	"database/sql"
	"fmt"
	executor "github.com/viant/datly/service/executor"
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/executor/extension"
	session "github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/xdatly/handler"
	http2 "github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/validator"
	"net/http"
)

type (
	Executor struct {
		executed        bool
		session         *session.Session
		executorSession *executor.Session
		handlerSession  *extension.Session
		*options
		view       *view.View
		connectors view.Connectors
		dataUnit   *expand.DataUnit
		tx         *sql.Tx
		response   http.ResponseWriter
	}

	DBProvider struct {
		db *sql.DB
	}
)

func (d *DBProvider) Db() (*sql.DB, error) {
	return d.db, nil
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
	if e.handlerSession != nil {
		return e.handlerSession, nil
	}

	var options = e.options.Clone(opts)
	e.session.Apply(session.WithTypes(options.Types...))
	e.session.Apply(session.WithEmbeddedFS(options.embedFS))

	res := e.view.GetResource()
	sess := extension.NewSession(
		extension.WithTemplateFlush(func(ctx context.Context) error {
			return e.Execute(ctx)
		}),
		extension.WithStater(e.session),
		extension.WithRedirect(e.redirect),
		extension.WithSql(e.newSqlService),
		extension.WithHttp(e.newHttp),
		extension.WithMessageBus(res.MessageBuses),
	)

	e.handlerSession = sess

	return sess, nil
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
	return &Service{
		txNotifier:    txStartedNotifier,
		dataUnit:      unit,
		options:       options,
		validator:     e.newValidator(),
		connectors:    e.connectors,
		mainConnector: e.view.Connector,
	}, nil
}

func (e *Executor) getDataUnit(options *sqlx.Options) (*expand.DataUnit, error) {
	if (options.WithDb == nil && options.WithTx == nil) || options.WithConnector == e.view.Connector.Name {
		return e.dataUnit, nil
	}

	if options.WithDb != nil {
		return expand.NewDataUnit(&DBProvider{db: options.WithDb}), nil
	}

	if options.WithConnector != "" {
		connector, err := e.connectors.Lookup(options.WithConnector)
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
	service := executor.New()
	var dbOptions []executor.DBOption
	if e.tx != nil {
		dbOptions = append(dbOptions, executor.WithTx(e.tx))
	}
	return service.ExecuteStmts(ctx, executor.NewViewDBSource(e.view), newSqlxIterator(e.dataUnit.Statements.Executable), dbOptions...)
}

func (e *Executor) ExpandAndExecute(ctx context.Context) (*executor.Session, error) {
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

func (e *Executor) txStarted(tx *sql.Tx) {
	e.tx = tx
}

func (e *Executor) redirect(ctx context.Context, route *http2.Route) (handler.Session, error) {
	//TODO reimplement it
	return nil, fmt.Errorf("not yey supported")
	//match, err := e.route.match(ctx, route)
	//if err != nil {
	//	return nil, err
	//}
	//
	//newExecutor := NewExecutor(match, e.request, e.response)
	//return newExecutor.HandlerSession(ctx)
}

func (e *Executor) newHttp() http2.Http {
	//TODO reimplement it
	return nil
}
