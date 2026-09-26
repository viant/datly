package reader

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	rsql "github.com/viant/datly/sql/builder"
	"github.com/viant/datly/sql/reader/readmeta"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	xreader "github.com/viant/xdatly/reader"
)

type outputRelationExecution struct {
	metrics   *viewRead
	service   *Service
	ctx       context.Context
	session   *Session
	input     reflect.Value
	binder    xhandler.Binder
	selectors invocationSelectors
	planned   *RelationPlan
	root      *cache.ParmetrizedQuery
	db        *sql.DB
	tx        *sql.Tx
	output    any
	field     *xshape.Accessor
}

func (s *Service) bindOutputRelations(ctx context.Context, session *Session, input reflect.Value, binder xhandler.Binder, selectors invocationSelectors, output any, relations []*RelationPlan) error {
	selected := make([]*RelationPlan, 0)
	holders := map[string]bool{}
	if session != nil && session.Artifact != nil && session.Artifact.OutputViewField != "" {
		holders[session.Artifact.OutputViewField] = true
	}
	for _, relation := range relations {
		if relation != nil && relation.Relation != nil && relation.Relation.IsOutput() {
			if holders[relation.Relation.Holder] {
				return fmt.Errorf("duplicate or conflicting output holder %s", relation.Relation.Holder)
			}
			holders[relation.Relation.Holder] = true
			selected = append(selected, relation)
		}
	}
	if len(selected) == 0 {
		return nil
	}
	if session == nil || session.Component == nil || session.SQL == nil {
		return fmt.Errorf("output relation reader requires an initialized session")
	}
	outputValue := reflect.ValueOf(output)
	if outputValue.Kind() != reflect.Ptr || outputValue.IsNil() || outputValue.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("output relations require a non-nil struct pointer, got %T", output)
	}
	root, err := s.nonWindowQuery(ctx, session, input, binder, selectors)
	if err != nil {
		return err
	}
	for _, relation := range selected {
		field := session.outputAccessors.fields[relation]
		if field == nil {
			return fmt.Errorf("output relation holder %s was not prepared", relation.Relation.Holder)
		}
		connection, err := viewConnection(ctx, session, relation.Target)
		if err != nil {
			return err
		}
		execution := &outputRelationExecution{
			service: s, ctx: ctx, session: session, input: input, binder: binder,
			selectors: selectors, planned: relation, root: root, db: connection.DB, tx: connection.Tx, field: field, output: output,
		}
		if err := execution.execute(); err != nil {
			return fmt.Errorf("read output relation %s: %w", relation.Relation.Name, err)
		}
	}
	return nil
}

func (s *Service) nonWindowQuery(ctx context.Context, session *Session, input reflect.Value, binder xhandler.Binder, selectors invocationSelectors) (*cache.ParmetrizedQuery, error) {
	rootSource := session.Component.RootSource()
	if rootSource == nil {
		return nil, fmt.Errorf("output relation root source is required")
	}
	rootPlan := session.Artifact.Root
	connection, err := viewConnection(ctx, session, rootPlan)
	if err != nil {
		return nil, err
	}
	rootSelector := selectors.forView(rootPlan.View)
	builder := rsql.NewBuilder()
	query, err := builder.Build(ctx,
		rsql.WithBuilderComponent(session.Component),
		rsql.WithBuilderView(rootPlan.View),
		rsql.WithBuilderCriteriaCompiler(rootPlan.Criteria),
		rsql.WithBuilderControls(rsql.NonWindowControls(rootSource.Controls, rootSelector)),
		rsql.WithBuilderSelector(rsql.NonWindowSelector(rootSelector)),
		rsql.WithBuilderInput(input.Elem()),
		rsql.WithBuilderParameterResolver(session.Parameters),
		rsql.WithBuilderTemplate(rootPlan.Template),
		rsql.WithBuilderBinder(binder),
		rsql.WithBuilderDialect(connection.Dialect),
		rsql.WithBuilderExcludePagination(true),
	)
	if err != nil {
		return nil, err
	}
	return builder.QueryMatcher(ctx, query,
		rsql.WithBuilderComponent(session.Component),
		rsql.WithBuilderView(rootPlan.View),
		rsql.WithBuilderSelector(rootSelector),
	)
}

func (e *outputRelationExecution) execute() (err error) {
	e.session.recorder.Pending(e.session.pendingScope, 1)
	defer e.session.recorder.Pending(e.session.pendingScope, -1)
	e.metrics = e.session.beginView(e.ctx, e.planned.Target.View)
	defer e.metrics.finish(&err)
	if e.planned.Target.Partitioner == nil {
		query, err := e.query(e.ctx, nil)
		if err != nil {
			return err
		}
		value, err := e.read(e.ctx, query)
		if err != nil {
			return err
		}
		if !value.value.IsValid() {
			return e.publish(nil, false)
		}
		return e.assign(value.value, value.fields)
	}
	return e.readPartitions()
}

func (e *outputRelationExecution) query(ctx context.Context, partition *xreader.Partition) (*cache.ParmetrizedQuery, error) {
	planned := e.planned
	if planned == nil || planned.Relation == nil || planned.Target == nil || planned.Target.View == nil || planned.Target.View.Spec.Source == nil {
		return nil, fmt.Errorf("output relation source is required")
	}
	relation := planned.Relation
	view := planned.Target.View
	if e.root == nil {
		return nil, fmt.Errorf("output relation parent query is required")
	}
	partitionOption := func(options []rsql.BuilderOption) []rsql.BuilderOption {
		if partition == nil {
			return options
		}
		return append(options, rsql.WithBuilderPartition(&rsql.PartitionInput{
			Table: partition.Table, Expression: partition.Expression, Args: append([]any(nil), partition.Args...),
		}))
	}
	if program := planned.Target.Template; program != nil {
		connection, err := viewConnection(ctx, e.session, planned.Target)
		if err != nil {
			return nil, err
		}
		options := []rsql.BuilderOption{
			rsql.WithBuilderComponent(e.session.Component),
			rsql.WithBuilderView(view),
			rsql.WithBuilderCriteriaCompiler(planned.Target.Criteria),
			rsql.WithBuilderSelector(e.selectors.forView(view)),
			rsql.WithBuilderProjection(viewProjection(view, e.selectors.forView(view))),
			rsql.WithBuilderInput(e.input.Elem()),
			rsql.WithBuilderParameterResolver(e.session.Parameters),
			rsql.WithBuilderTemplate(program),
			rsql.WithBuilderBinder(e.binder),
			rsql.WithBuilderDialect(connection.Dialect),
			rsql.WithBuilderParentQuery(e.root),
			rsql.WithBuilderParentSelector(e.selectors.forView(e.session.Artifact.Root.View)),
		}
		return rsql.NewBuilder().Build(ctx, partitionOption(options)...)
	}
	preparedBinder := rsql.PreparedRelationBinder{
		Component:         e.session.Component,
		Relation:          relation,
		RootNonWindowSQL:  e.root.SQL,
		RootArgs:          toAnySlice(e.root.Args),
		ParameterResolver: e.session.Parameters,
	}
	sqlText, args, err := preparedBinder.Bind()
	if err != nil {
		return nil, err
	}
	selector := e.selectors.forView(view)
	options := []rsql.BuilderOption{
		rsql.WithBuilderView(view),
		rsql.WithBuilderCriteriaCompiler(planned.Target.Criteria),
		rsql.WithBuilderSelector(selector),
		rsql.WithBuilderProjection(viewProjection(view, selector)),
		rsql.WithBuilderInput(e.input.Elem()),
		rsql.WithBuilderParameterResolver(e.session.Parameters),
	}
	return rsql.NewBuilder().ShapeBound(
		&cache.ParmetrizedQuery{SQL: sqlText, Args: toInterfaceSlice(args)},
		partitionOption(options)...,
	)
}

func (e *outputRelationExecution) read(ctx context.Context, query *cache.ParmetrizedQuery) (outputRead, error) {
	if query == nil || query.SQL == "" {
		return outputRead{}, fmt.Errorf("output relation query is required")
	}
	view := e.planned.Target.View
	rowType := e.field.Type()
	if rowType.Kind() == reflect.Ptr {
		rowType = rowType.Elem()
	}
	if rowType.Kind() != reflect.Struct {
		return outputRead{}, fmt.Errorf("output relation holder must be a struct or pointer to struct, got %s", e.field.Type())
	}
	newRow := func() interface{} { return reflect.New(rowType).Interface() }
	scan := newReaderOptions(e.session, view).rows(newRow, sqlxio.NewResolver().Resolve)
	var result outputRead
	visitor := newRowHookVisitor(ctx, view, nil, nil)
	visitor.decoder = scan.decoder
	err := scan.query(ctx, rowQuery{db: e.db, tx: e.tx, query: query, read: e.metrics, visit: func(row interface{}) error {
		if result.value.IsValid() {
			return nil
		}
		row, err := visitor.VisitRow(row)
		if err != nil {
			return err
		}
		result.value = reflect.ValueOf(row)
		if scan.evidence != nil {
			result.fields = scan.evidence.fields
		}
		return nil
	}})
	return result, err
}

func (e *outputRelationExecution) readPartitions() error {
	provider, ok := e.planned.Target.Partitioner.(xreader.ReducerProvider)
	if !ok {
		return fmt.Errorf("partitioned output relation %s requires a reducer", viewName(e.planned.Target.View))
	}
	partitions, err := e.service.resolvePartitions(e.ctx, e.planned.Target, e.db)
	if err != nil {
		return err
	}
	if len(partitions.items) == 0 {
		return e.publish(nil, false)
	}
	values := make([]reflect.Value, len(partitions.items))
	if err := partitions.run(e.ctx, func(workCtx context.Context, index int, partition xreader.Partition) error {
		query, err := e.query(workCtx, &partition)
		if err != nil {
			return err
		}
		value, err := e.read(workCtx, query)
		if err != nil {
			return err
		}
		if value.value.IsValid() {
			values[index] = value.value
		}
		return nil
	}); err != nil {
		return err
	}
	rowType := e.field.Type()
	if rowType.Kind() == reflect.Ptr {
		rowType = rowType.Elem()
	}
	rows := reflect.MakeSlice(reflect.SliceOf(rowType), 0, len(values))
	for _, value := range values {
		if !value.IsValid() {
			continue
		}
		if value.Kind() == reflect.Ptr {
			value = value.Elem()
		}
		rows = reflect.Append(rows, value)
	}
	reducer := provider.Reducer(e.ctx)
	if reducer == nil {
		return fmt.Errorf("partition reducer is nil for output relation %s", viewName(e.planned.Target.View))
	}
	reduced, err := reducer.Reduce(e.ctx, rows.Interface())
	if err != nil {
		return fmt.Errorf("reduce partitions for output relation %s: %w", viewName(e.planned.Target.View), err)
	}
	return e.assign(reflect.ValueOf(reduced), nil)
}

func (e *outputRelationExecution) assign(value reflect.Value, fields *readmeta.Fields) error {
	if !value.IsValid() {
		return fmt.Errorf("output relation %s returned an invalid value", viewName(e.planned.Target.View))
	}
	converted, ok := convertCollectorItem(value, e.field.Type())
	if !ok {
		return fmt.Errorf("output relation %s result %s cannot populate %s", viewName(e.planned.Target.View), value.Type(), e.field.Type())
	}
	if err := e.field.Set(e.output, converted.Interface()); err != nil {
		return err
	}
	assigned, err := e.field.Get(e.output)
	if err != nil {
		return err
	}
	if assigned.Kind() == reflect.Ptr && assigned.IsNil() {
		return e.publish(nil, false)
	}
	if assigned.Kind() == reflect.Ptr {
		before := assigned.Interface()
		if runOnRelation(e.ctx, assigned.Interface()) {
			assigned, err = e.field.Get(e.output)
			if err != nil {
				return err
			}
			if assigned.Interface() != before {
				fields = nil
			}
		}
	} else {
		if runOnRelation(e.ctx, assigned.Addr().Interface()) {
			fields = nil
			assigned, err = e.field.Get(e.output)
			if err != nil {
				return err
			}
		}
	}
	if !assigned.CanAddr() || assigned.Kind() == reflect.Ptr && assigned.IsNil() {
		return e.publish(nil, false)
	}
	return e.publish(fields, true)
}

func toInterfaceSlice[T any](input []T) []interface{} {
	if len(input) == 0 {
		return nil
	}
	result := make([]interface{}, len(input))
	for i, item := range input {
		result[i] = item
	}
	return result
}
