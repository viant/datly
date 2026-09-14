package builder

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/criteria"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlparser"
	sqltext "github.com/viant/sqlparser/source"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/metadata/info"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

// Builder assembles reader queries from resolved view, selector, relation,
// template, partition, and invocation metadata.
type Builder struct{}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) CacheSQL(ctx context.Context, opts ...BuilderOption) (*cache.ParmetrizedQuery, error) {
	options := newBuilderOptions(opts...)
	window, err := b.resolveControls(options, false)
	if err != nil {
		return nil, err
	}
	cacheSelector := NonWindowSelector(options.selector)
	cacheControls := NonWindowControls(options.controls, options.selector)
	cacheMatcherIn := options.matcherIn
	if len(cacheMatcherIn) == 0 {
		if len(options.compositeColumns) == 0 {
			cacheMatcherIn = options.positionalArgs
		}
	}
	cacheOptions := []BuilderOption{WithBuilderComponent(options.component)}
	if options.view != nil {
		cacheOptions = append(cacheOptions, WithBuilderView(options.view))
	} else if options.source != nil {
		cacheOptions = append(cacheOptions, WithBuilderSource(options.source))
	}
	cacheOptions = append(cacheOptions,
		WithBuilderCriteriaCompiler(options.criteriaCompiler),
		WithBuilderSQL(options.sqlText),
		WithBuilderControls(cacheControls),
		WithBuilderSelector(cacheSelector),
		WithBuilderInput(options.input),
		WithBuilderParameterResolver(options.parameterResolver),
		WithBuilderRelation(options.relation),
		WithBuilderDialect(options.dialect),
		WithBuilderMatcher(options.matcherBy, cacheMatcherIn),
		WithBuilderProjection(options.projection),
		WithBuilderTemplate(options.template),
		WithBuilderBinder(options.binder),
		WithBuilderSkipRelationFilter(true),
		WithBuilderExcludePagination(true),
	)
	if options.parentQuery != nil {
		cacheOptions = append(cacheOptions, WithBuilderParentQuery(options.parentQuery))
	}
	if options.parentSelector != nil {
		cacheOptions = append(cacheOptions, WithBuilderParentSelector(options.parentSelector))
	}
	if options.selectorPolicy != nil {
		cacheOptions = append(cacheOptions, WithBuilderSelectorPolicy(options.selectorPolicy))
	}
	result, err := b.Build(ctx, cacheOptions...)
	if err != nil {
		return nil, err
	}
	if len(options.compositeColumns) > 1 {
		result.By = ""
		result.In = nil
		result.ByColumns = options.matcherColumns(options.compositeColumns)
		result.InTuples = cloneInterfaceRows(options.compositeRows)
	}
	applyMatcherWindow(result, window)
	return result, nil
}

// QueryMatcher describes relation-local key/window semantics for the exact
// query executed by the reader. Indexed warmup may use CacheSQL separately;
// this matcher remains valid for ordinary sqlx lazy-cache replay even when an
// authored source has no explicit removable parent-key macro.
func (b *Builder) QueryMatcher(ctx context.Context, query *cache.ParmetrizedQuery, opts ...BuilderOption) (*cache.ParmetrizedQuery, error) {
	if query == nil {
		return nil, fmt.Errorf("relation query is required")
	}
	options := newBuilderOptions(opts...)
	window, err := b.resolveControls(options, false)
	if err != nil {
		return nil, err
	}
	result := &cache.ParmetrizedQuery{
		SQL:  query.SQL,
		Args: append([]interface{}(nil), query.Args...),
	}
	if len(options.compositeColumns) > 1 {
		result.ByColumns = options.matcherColumns(options.compositeColumns)
		result.InTuples = cloneInterfaceRows(options.compositeRows)
	} else {
		if options.matcherBy != "" {
			result.By = options.matcherColumn(options.matcherBy)
		} else if options.relation != nil && options.relation.Of != nil && len(options.relation.Of.On) > 0 {
			link := options.relation.Of.On[0]
			if link.Column != "" {
				result.By = options.matcherColumn(link.Column)
			} else {
				result.By = link.Field
			}
		}
		if len(options.matcherIn) > 0 {
			result.In = interfaceSlice(options.matcherIn)
		} else {
			result.In = interfaceSlice(options.positionalArgs)
		}
	}
	applyMatcherWindow(result, window)
	return result, nil
}

func (b *Builder) Build(ctx context.Context, opts ...BuilderOption) (*cache.ParmetrizedQuery, error) {
	options := newBuilderOptions(opts...)
	if options.template != nil {
		viewInput := sqltemplate.ViewInput{
			Dialect:               options.dialect,
			ParentValues:          options.positionalArgs,
			ParentCompositeValues: options.compositeRows,
			ExcludeParent:         options.skipRelationFilter,
		}
		if options.selector != nil {
			viewInput.Limit = options.selector.Limit
			viewInput.Offset = options.selector.Offset
			viewInput.Page = options.selector.Page
		}
		if options.parentQuery != nil {
			viewInput.NonWindowSQL = options.parentQuery.SQL
			viewInput.NonWindowArgs = append([]any(nil), options.parentQuery.Args...)
			viewInput.Limit = options.parentQuery.Limit
			viewInput.Offset = options.parentQuery.Offset
			if options.parentSelector != nil {
				viewInput.Page = options.parentSelector.Page
			}
		}
		evaluated, err := options.template.Evaluate(ctx, sqltemplate.Invocation{Input: options.input, View: viewInput, Binder: options.binder})
		if err != nil {
			return nil, err
		}
		options.sqlText = evaluated.SQL
		options.templateArgs = append([]any(nil), evaluated.Args...)
		options.templateParentBindings = evaluated.ParentBindings
		if len(options.templateArgs) > 0 && !options.templateParentBindings && (len(options.positionalArgs) > 0 || len(options.compositeRows) > 0) {
			return nil, fmt.Errorf("SQL template bindings cannot be combined with relation positional bindings")
		}
	}
	if err := b.resolveSourceSQL(options); err != nil {
		return nil, err
	}
	if err := b.prepareCriteria(options); err != nil {
		return nil, err
	}
	prepared, err := options.prepareProjectionSource()
	if err != nil {
		return nil, err
	}
	options.sqlText = prepared.sql
	controls, err := b.resolveControls(options, options.excludePagination)
	if err != nil {
		return nil, err
	}
	if err := b.validateProjection(options); err != nil {
		return nil, err
	}
	sourceSQL := options.sqlText
	if options.selector != nil && strings.TrimSpace(options.selector.OrderBy) != "" && len(options.projection) == 0 {
		sourceSQL, err = (dsql.SelectorProjection{SQL: sourceSQL, View: options.view}).Expand()
		if err != nil {
			return nil, err
		}
	}
	sqlText, err := dsql.ApplySelectorProjection(sourceSQL, options.projection, options.view)
	if err != nil {
		return nil, err
	}
	sqlText = dsql.PrepareExecutableSQL(sqlText, controls)
	relationFilter := relationFilter{
		relation: options.relation, positionalArgs: options.positionalArgs,
		compositeColumns: options.compositeColumns, compositeRows: options.compositeRows, dialect: options.dialect,
	}
	compositeInjected := prepared.compositeInjected
	if !options.skipRelationFilter && !prepared.parentHandled && !compositeInjected {
		sqlText, compositeInjected = relationFilter.applyColumnIn(sqlText, prepared.hadRelationCriteria)
	}

	hadExplicitSelectorCriteria := containsSelectorCriteriaToken(sqlText)
	bindingPositionalArgs := options.positionalArgs
	if len(options.templateArgs) > 0 || options.templateParentBindings {
		bindingPositionalArgs = options.templateArgs
	} else if prepared.macroCount > 0 {
		bindingPositionalArgs = prepared.macroArgs
	}
	if len(options.compositeColumns) > 0 && (compositeInjected || prepared.hadRelationCriteria) {
		bindingPositionalArgs = flattenCompositeArgs(options.compositeRows)
	}
	var (
		boundSQL string
		args     []any
	)
	if len(bindingPositionalArgs) > 0 {
		sqlText = expandPositionalInClause(sqlText, len(bindingPositionalArgs))
	}
	boundSQL, args, err = bindSelectorCriteriaSQL(sqlText, options.parameterResolver, options.selector, bindingPositionalArgs)
	if err != nil {
		return nil, err
	}
	boundSQL, args = appendAutoSelectorCriteria(boundSQL, args, options.selector, hadExplicitSelectorCriteria)
	if len(options.compositeColumns) > 0 {
		if !options.skipRelationFilter && !prepared.parentHandled && !compositeInjected && !prepared.hadRelationCriteria {
			boundSQL, args = appendCompositeWhere(boundSQL, args, options.compositeRows, options.compositeColumns, options.dialect)
		}
	}
	boundSQL, args, err = applyPartition(boundSQL, args, options.source, options.partition)
	if err != nil {
		return nil, err
	}
	result := &cache.ParmetrizedQuery{
		SQL:  boundSQL,
		Args: interfaceSlice(args),
	}
	if options.relation != nil && len(options.relation.Of.On) > 0 && options.matcherBy == "" {
		link := options.relation.Of.On[0]
		if link.Column != "" {
			result.By = options.matcherColumn(link.Column)
		} else {
			result.By = link.Field
		}
	}
	if options.matcherBy != "" {
		result.By = options.matcherColumn(options.matcherBy)
	}
	if len(options.matcherIn) > 0 {
		result.In = interfaceSlice(options.matcherIn)
	} else if len(options.positionalArgs) > 0 {
		result.In = interfaceSlice(options.positionalArgs)
	}
	return result, nil
}

func (o *builderOptions) matcherColumns(columns []string) []string {
	result := make([]string, 0, len(columns))
	for _, column := range columns {
		result = append(result, o.matcherColumn(column))
	}
	return result
}

func (o *builderOptions) matcherColumn(column string) string {
	sourceColumn := strings.TrimSpace(column)
	column = strings.TrimSpace(column)
	if parts, err := sqlparser.TableIdentifierParts(column); err == nil {
		column = parts[len(parts)-1]
	}
	column = strings.TrimSpace(column)
	// A generated table SELECT aliases columns only with canonical mapping authority.
	// Native row/cache matchers consume result names, while WHERE links keep
	// their physical column names. Authored SQL owns its own projection.
	if o.view != nil && o.source != nil && o.source.Table != "" && o.source.SQL == "" && (o.sqlText == "" || o.tableProjection) && o.template == nil {
		for _, projected := range o.view.Columns {
			if projected != nil && ((dsql.ProjectionNames{projected.Column}).Matches(sourceColumn) || strings.EqualFold(projected.Column, column)) && projected.Name != "" {
				resolved, err := (dsql.SelectorProjection{View: o.view}).TableColumn(projected)
				if err == nil {
					if parts, err := sqlparser.TableIdentifierParts(resolved.Name); err == nil && len(parts) == 1 {
						return parts[0]
					}
				}
			}
		}
	}
	return column
}

func cloneInterfaceRows(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return nil
	}
	result := make([][]interface{}, len(rows))
	for i, row := range rows {
		result[i] = append([]interface{}(nil), row...)
	}
	return result
}

func interfaceSlice[T any](input []T) []interface{} {
	if len(input) == 0 {
		return nil
	}
	result := make([]interface{}, len(input))
	for i, item := range input {
		result[i] = item
	}
	return result
}

func expandPositionalInClause(sqlText string, argCount int) string {
	return sqlx.ParseParameters(sqlText).ExpandSinglePositional(argCount)
}

func appendCompositeWhere(sqlText string, args []any, compositeRows [][]interface{}, columns []string, dialect *info.Dialect) (string, []any) {
	if len(columns) == 0 {
		return sqlText, args
	}
	expression := "1 = 0"
	if len(compositeRows) == 0 {
		clause := " WHERE " + expression
		if sqltext.HasTopLevelClause(sqlText, "where") {
			clause = " AND " + expression
		}
		return insertRelationClause(sqlText, clause), args
	}
	expression = renderCompositeIn(dialect, columns, len(compositeRows))
	clause := " WHERE " + expression
	if sqltext.HasTopLevelClause(sqlText, "where") {
		clause = " AND " + expression
	}
	insertAt := sqltext.CriteriaBoundary(sqlText)
	argAt := sqlx.ParseParameters(sqlText[:insertAt]).Count()
	values := flattenCompositeArgs(compositeRows)
	resultArgs := make([]any, 0, len(args)+len(values))
	resultArgs = append(resultArgs, args[:argAt]...)
	resultArgs = append(resultArgs, values...)
	resultArgs = append(resultArgs, args[argAt:]...)
	return insertRelationClause(sqlText, clause), resultArgs
}

func flattenCompositeArgs(compositeRows [][]interface{}) []any {
	if len(compositeRows) == 0 {
		return nil
	}
	var result []any
	for _, row := range compositeRows {
		for _, value := range row {
			result = append(result, value)
		}
	}
	return result
}

func renderCompositeIn(dialect *info.Dialect, columns []string, rowCount int) string {
	return dialect.CompositeIn(columns, rowCount)
}

type BuilderOption func(*builderOptions)

type builderOptions struct {
	component              *spec.Component
	source                 *spec.ViewSource
	view                   *data.View
	sqlText                string
	tableProjection        bool
	controls               *spec.ViewControls
	selector               *xstate.Selector
	input                  reflect.Value
	parameterResolver      sqlx.ParameterResolver
	relation               *data.Relation
	dialect                *info.Dialect
	positionalArgs         []any
	compositeRows          [][]interface{}
	compositeColumns       []string
	matcherBy              string
	matcherIn              []any
	projection             []string
	skipRelationFilter     bool
	selectorPolicy         *spec.Selector
	criteriaCompiler       *criteria.Compiler
	excludePagination      bool
	partition              *PartitionInput
	template               sqltemplate.Evaluator
	binder                 xhandler.Binder
	parentQuery            *cache.ParmetrizedQuery
	parentSelector         *xstate.Selector
	templateArgs           []any
	templateParentBindings bool
}

func newBuilderOptions(options ...BuilderOption) *builderOptions {
	result := &builderOptions{}
	for _, option := range options {
		if option != nil {
			option(result)
		}
	}
	return result
}

func WithBuilderSQL(sqlText string) BuilderOption {
	return func(o *builderOptions) {
		o.sqlText = sqlText
	}
}

// WithBuilderSource supplies immutable SQL source metadata as one unit. It is
// preferred over passing SQL text and controls separately when a view source
// is available.
func WithBuilderSource(source *spec.ViewSource) BuilderOption {
	return func(o *builderOptions) {
		o.source = source
		if source == nil {
			return
		}
		o.sqlText = source.SQL
		o.controls = source.Controls
	}
}

// WithBuilderView supplies the canonical static/runtime-separated view model.
// SQL construction reads static source/column/selector metadata only.
func WithBuilderView(view *data.View) BuilderOption {
	return func(o *builderOptions) {
		o.view = view
		if view == nil {
			return
		}
		o.source = view.Spec.Source
		o.selectorPolicy = view.Spec.Selector
		if view.Spec.Source != nil {
			o.sqlText = view.Spec.Source.SQL
			o.controls = view.Spec.Source.Controls
		}
	}
}

func WithBuilderComponent(component *spec.Component) BuilderOption {
	return func(o *builderOptions) {
		o.component = component
	}
}

// WithBuilderTemplate injects the precompiled root SQL-template evaluator.
func WithBuilderTemplate(evaluator sqltemplate.Evaluator) BuilderOption {
	return func(o *builderOptions) {
		o.template = evaluator
	}
}

// WithBuilderBinder supplies the invocation-scoped DI binder used by compiled
// SQL parameter contracts. The builder never retains it beyond Build.
func WithBuilderBinder(binder xhandler.Binder) BuilderOption {
	return func(o *builderOptions) {
		o.binder = binder
	}
}

// WithBuilderParentQuery supplies the invocation-scoped non-window query
// exposed through $View.NonWindowSQL in a compiled child-view template.
func WithBuilderParentQuery(query *cache.ParmetrizedQuery) BuilderOption {
	return func(o *builderOptions) {
		if query == nil {
			o.parentQuery = nil
			return
		}
		o.parentQuery = &cache.ParmetrizedQuery{
			SQL:    query.SQL,
			Args:   append([]interface{}(nil), query.Args...),
			Limit:  query.Limit,
			Offset: query.Offset,
		}
	}
}

// WithBuilderParentSelector supplies parent invocation state that is not part
// of sqlx's parametrized query, currently the authored page number.
func WithBuilderParentSelector(selector *xstate.Selector) BuilderOption {
	return func(o *builderOptions) {
		if selector == nil {
			o.parentSelector = nil
			return
		}
		cloned := *selector
		o.parentSelector = &cloned
	}
}

func WithBuilderControls(controls *spec.ViewControls) BuilderOption {
	return func(o *builderOptions) {
		o.controls = controls
	}
}

func WithBuilderSelector(selector *xstate.Selector) BuilderOption {
	return func(o *builderOptions) {
		o.selector = selector
	}
}

// WithBuilderSelectorPolicy supplies immutable per-view selector policy. When
// omitted, the builder derives it from WithBuilderComponent when available.
func WithBuilderSelectorPolicy(policy *spec.Selector) BuilderOption {
	return func(o *builderOptions) {
		o.selectorPolicy = policy
	}
}

func WithBuilderInput(input reflect.Value) BuilderOption {
	return func(o *builderOptions) {
		o.input = input
	}
}

// WithBuilderParameterResolver supplies invocation-scoped named SQL values.
// SQLX remains the sole owner of placeholder parsing and argument ordering.
func WithBuilderParameterResolver(resolver sqlx.ParameterResolver) BuilderOption {
	return func(o *builderOptions) {
		o.parameterResolver = resolver
	}
}

func WithBuilderRelation(relation *data.Relation) BuilderOption {
	return func(o *builderOptions) {
		o.relation = relation
	}
}

func WithBuilderDialect(dialect *info.Dialect) BuilderOption {
	return func(o *builderOptions) {
		o.dialect = dialect
	}
}

func WithBuilderPositionalArgs(args []any) BuilderOption {
	return func(o *builderOptions) {
		o.positionalArgs = append([]any(nil), args...)
	}
}

func WithBuilderCompositeArgs(columns []string, rows [][]interface{}) BuilderOption {
	return func(o *builderOptions) {
		o.compositeColumns = append([]string(nil), columns...)
		if len(rows) == 0 {
			o.compositeRows = nil
			return
		}
		o.compositeRows = make([][]interface{}, len(rows))
		for i, row := range rows {
			o.compositeRows[i] = append([]interface{}(nil), row...)
		}
	}
}

func WithBuilderMatcher(by string, in []any) BuilderOption {
	return func(o *builderOptions) {
		o.matcherBy = by
		o.matcherIn = append([]any(nil), in...)
	}
}

func WithBuilderProjection(columns []string) BuilderOption {
	return func(o *builderOptions) {
		o.projection = append([]string(nil), columns...)
	}
}

func WithBuilderSkipRelationFilter(skip bool) BuilderOption {
	return func(o *builderOptions) {
		o.skipRelationFilter = skip
	}
}

func WithBuilderExcludePagination(exclude bool) BuilderOption {
	return func(o *builderOptions) {
		o.excludePagination = exclude
	}
}

func WithBuilderPartition(partition *PartitionInput) BuilderOption {
	return func(o *builderOptions) {
		o.partition = partition.Clone()
	}
}
