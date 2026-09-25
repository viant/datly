package reader

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/builder"
	"github.com/viant/sqlx"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/metadata/info"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
)

func (e *Execution) PrepareQuery(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*dexec.PreparedQuery, error) {
	if e == nil {
		return nil, fmt.Errorf("reader execution is required")
	}
	session := e.session()
	if err := session.Init(); err != nil {
		return nil, err
	}
	session.initMetrics(e.service.recorder)
	value := reflect.ValueOf(input)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Type() != session.InputType {
		return nil, fmt.Errorf("query input must be *%s", session.InputType)
	}
	root := session.Artifact.Root
	if root.Partitioner != nil {
		return nil, fmt.Errorf("query composition does not support partitioned source roots")
	}
	selectors, err := resolveInvocationSelectors(ctx, session, value, binder)
	if err != nil {
		return nil, err
	}
	connection, err := viewConnection(ctx, session, root)
	if err != nil {
		return nil, err
	}
	selector := builder.NonWindowSelector(selectors.forView(root.View))
	query, err := builder.NewBuilder().Build(ctx,
		builder.WithBuilderComponent(session.Component), builder.WithBuilderView(root.View),
		builder.WithBuilderCriteriaCompiler(root.Criteria),
		builder.WithBuilderSelector(selector), builder.WithBuilderProjection(viewProjection(root.View, selector)),
		builder.WithBuilderInput(value.Elem()), builder.WithBuilderParameterResolver(resolver),
		builder.WithBuilderTemplate(root.Template), builder.WithBuilderBinder(binder),
		builder.WithBuilderDialect(connection.Dialect), builder.WithBuilderExcludePagination(true),
	)
	if err != nil {
		return nil, err
	}
	return &dexec.PreparedQuery{SQL: query.SQL, Args: append([]any(nil), query.Args...), Projection: &projectionReader{db: connection.DB, tx: connection.Tx, dialect: connection.Dialect, component: session.Component, view: root.View, recorder: session.recorder, metricScope: session.metricScope, retry: readRetry{source: session.SQL, connector: root.Connector}}}, nil
}

// projectionReader is request-local. SQLX owns typed row mapping; source caches,
// relations, dictionaries and hooks do not apply to a new wrapper projection.
type projectionReader struct {
	retry       readRetry
	metricScope string
	component   *spec.Component
	view        *data.View
	recorder    *observability.Recorder
	db          *sql.DB
	tx          *sql.Tx
	dialect     *info.Dialect
}

func (r *projectionReader) ReadProjection(ctx context.Context, request dexec.ProjectionRequest) (_ any, err error) {
	if request.RowType == nil || request.RowType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("projection requires a struct row type")
	}
	// The wrapper combines arguments from every prepared frame and may add its
	// own. Validate the final total, not just each independently valid source.
	if err := (parameterBudget{dialect: r.dialect}).check(len(request.Args)); err != nil {
		return nil, err
	}
	rows := reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(request.RowType)), 0, 0)
	session := &Session{Component: r.component, recorder: r.recorder, metricScope: r.metricScope}
	if session.Component == nil {
		session.Component = &spec.Component{}
	}
	if session.recorder == nil {
		session.recorder = observability.NewRecorder(nil)
	}
	observation := session.beginView(ctx, r.view)
	session.recorder.Pending(observation.scope, 1)
	defer session.recorder.Pending(observation.scope, -1)
	defer observation.finish(&err)
	scan := rowRead{newRow: func() any { return reflect.New(request.RowType).Interface() }, options: []sqlxread.Option{sqlxread.WithRetry(r.retry.policy())}}
	err = scan.query(ctx, rowQuery{db: r.db, tx: r.tx, query: &cache.ParmetrizedQuery{SQL: request.SQL, Args: request.Args}, read: observation, visit: func(row any) error { rows = reflect.Append(rows, reflect.ValueOf(row)); return nil }})
	if err != nil {
		return nil, err
	}
	return rows.Interface(), nil
}
