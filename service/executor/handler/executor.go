package handler

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	executor "github.com/viant/datly/service/executor"
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/executor/extension"
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
	"net/http"
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
		tx         *sql.Tx
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
	res := e.view.GetResource()
	sess := extension.NewSession(
		extension.WithTemplateFlush(func(ctx context.Context) error {
			return e.Execute(ctx)
		}),
		extension.WithStater(aSession),
		extension.WithRedirect(e.redirect),
		extension.WithSql(e.newSqlService),
		extension.WithHttp(e.newHttp),
		extension.WithAuth(e.newAuth),
		extension.WithMessageBus(res.MessageBuses),
	)
	return sess
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
	if (options.WithDb == nil && options.WithTx == nil) && options.WithConnector == e.view.Connector.Name {
		return e.dataUnit, nil
	}

	if options.WithDb != nil {
		return expand.NewDataUnit(&DBProvider{db: options.WithDb}), nil
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
		db, err := connector.DB()
		if err != nil {
			return nil, err
		}

		unit := expand.NewDataUnit(&DBProvider{db: db})

		e.dataUnits[options.WithConnector] = unit
		return unit, nil
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

	for _, unit := range e.dataUnits {
		dbSource := &DbSource{}
		dbSource.db, _ = unit.MetaSource.Db()
		if err := service.ExecuteStmts(ctx, dbSource, newSqlxIterator(unit.Statements.Executable)); err != nil {
			return err
		}
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

func (e *Executor) redirect(ctx context.Context, route *http2.Route, opts ...hstate.Option) (handler.Session, error) {
	registry := e.session.Registry()
	if registry == nil {
		return nil, fmt.Errorf("registry was empty")
	}
	aComponent, err := registry.Lookup(ctx, contract.NewPath(route.Method, route.URL))
	if err != nil {
		return nil, err
	}
	originalRequest, _ := e.session.HttpRequest(ctx, e.session.Clone())

	request, _ := http.NewRequest(route.Method, route.URL, nil)
	if originalRequest != nil {
		request.Header = originalRequest.Header
	}
	stateOptions := hstate.NewOptions(opts...)

	unmarshal := aComponent.UnmarshalFunc(request)
	locatorOptions := append(aComponent.LocatorOptions(request, hstate.NewForm(), unmarshal))
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
		session.WithRegistry(registry),
	)

	err = aSession.InitKinds(state.KindComponent, state.KindHeader, state.KindRequestBody, state.KindForm, state.KindQuery)
	if err != nil {
		return nil, err
	}
	ctx = aSession.Context(ctx, true)
	anExecutor := NewExecutor(aComponent.View, aSession)
	return anExecutor.NewHandlerSession(ctx)
}

func (e *Executor) newHttp() http2.Http {
	return NewHttp(e, e.view.GetResource())
}

func (e *Executor) newAuth() hauth.Auth {
	return NewAuth(e.auth)
}
