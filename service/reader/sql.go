package reader

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/reader/metadata"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx/io/read/cache"
)

const (
	selectFragment      = "SELECT "
	separatorFragment   = ", "
	fromFragment        = " FROM "
	asFragment          = " AS "
	limitFragment       = " LIMIT "
	orderByFragment     = " ORDER BY "
	offsetFragment      = " OFFSET "
	inFragment          = " IN ("
	andFragment         = " AND ("
	placeholderFragment = "?"
	encloseFragment     = ")"
)

type (
	//Builder represent SQL Builder
	Builder struct{}

	//BatchData groups view needed to use various view.MatchStrategy
)

// NewBuilder creates Builder instance
func NewBuilder() *Builder {
	return &Builder{}
}

// Build builds SQL Select statement
func (b *Builder) Build(ctx context.Context, opts ...BuilderOption) (*cache.ParmetrizedQuery, error) {
	options := newBuilderOptions(opts...)
	aView := options.view
	statelet := options.statelet
	// guard against nil batchData passed by callers
	var batchData view.BatchData
	if options.batchData != nil {
		batchData = *options.batchData
	}
	relation := options.relation
	exclude := options.exclude
	parent := options.parent
	partitions := options.partition
	expander := options.expander

	// ensure non-nil statelet to avoid nil deref on Template usage
	if statelet == nil {
		statelet = view.NewStatelet()
		statelet.Init(aView)
	}

	state, err := aView.Template.EvaluateSource(ctx, statelet.Template, parent, &batchData, expander)

	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, fmt.Errorf("failed to evaluate state for view %v, state was nil", aView.Name)
	}
	if state.Expanded == "" {
		return nil, fmt.Errorf("failed to evaluate expanded for view %vm statelet was nil", aView.Name)
	}
	if len(state.Filters) > 0 {
		statelet.AppendFilters(state.Filters)
	}

	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	projectedColumns, err := b.appendColumns(&sb, aView, statelet)
	if err != nil {
		return nil, err
	}
	if aView.Groupable {
		if state.Expanded, err = b.rewriteGroupBy(state.Expanded, aView.Columns, projectedColumns); err != nil {
			return nil, err
		}
	}
	if aView.Template.IsActualTemplate() && aView.ShouldTryDiscover() {
		state.Expanded = metadata.EnrichWithDiscover(state.Expanded, true)
	}

	if err = b.appendRelationColumn(&sb, aView, statelet, relation); err != nil {
		return nil, err
	}

	sb.WriteString(fromFragment)
	sb.WriteString(state.Expanded)
	b.appendViewAlias(&sb, aView)

	columnsInMeta := hasKeyword(state.Expanded, keywords.ColumnsIn)
	commonParams := view.CriteriaParam{}

	criteriaMeta := hasKeyword(state.Expanded, keywords.Criteria)
	hasCriteria := criteriaMeta.has()

	b.updateColumnsIn(&commonParams, &batchData, exclude)

	if err = b.updatePagination(&commonParams, aView, statelet, exclude); err != nil {
		return nil, err
	}

	if err = b.updateCriteria(&commonParams, columnsInMeta); err != nil {
		return nil, err
	}

	if partitions != nil && partitions.Expression != "" {
		if commonParams.WhereClause != "" {
			commonParams.WhereClause += " AND "
		}
		commonParams.WhereClause += partitions.Expression
		commonParams.WhereClause += " "
		if len(partitions.Placeholders) > 0 {
			commonParams.WhereClauseParameters = partitions.Placeholders
		} else {
			fmt.Printf("No partition found for %v\n", aView.Name)
		}
	}

	if !hasCriteria {
		sb.WriteString(" ")
		if strings.TrimSpace(commonParams.WhereClause) != "" {
			sb.WriteString(keywords.WhereCriteria)
			sb.WriteString(" ")
			sb.WriteString(keywords.AndSelectorCriteria)
		} else {
			sb.WriteString(" ")
			sb.WriteString(keywords.WhereSelectorCriteria)
			sb.WriteString(" ")
		}
	} else {
		sb.WriteString(" ")
		sb.WriteString(keywords.WhereSelectorCriteria)
	}

	hasPagination := strings.Contains(state.Expanded, keywords.Pagination)
	if !hasPagination && commonParams.Pagination != "" {
		sb.WriteString(" ")
		sb.WriteString(keywords.Pagination)
		sb.WriteString(" ")
	}

	var placeholders []interface{}

	SQL, err := aView.Expand(&placeholders, sb.String(), statelet, commonParams, &batchData, state.DataUnit)
	if err != nil {
		return nil, err
	}
	if partitions != nil && partitions.Table != "" && aView.Table != "" {
		SQL = strings.ReplaceAll(SQL, aView.Table, partitions.Table)
	}
	parametrizedQuery := &cache.ParmetrizedQuery{
		SQL:  SQL,
		Args: placeholders,
	}
	if os.Getenv("DATLY_DEBUG_SQL_BUILDER") == "1" {
		fmt.Printf("[SQL BUILDER] view=%s sql=%s args=%#v state=%s\n", aView.Name, SQL, placeholders, state.Expanded)
	}

	if exclude.ColumnsIn && relation != nil {
		parametrizedQuery.By = shared.FirstNotEmpty(relation.Of.On[0].Field, relation.Of.On[0].Column)
		parametrizedQuery.In = batchData.ValuesBatch
	}

	if exclude.Pagination {
		parametrizedQuery.Offset = statelet.Offset
		parametrizedQuery.Limit = actualLimit(aView, statelet)
	}

	return parametrizedQuery, err
}

func (b *Builder) appendColumns(sb *strings.Builder, aView *view.View, selector *view.Statelet) ([]*view.Column, error) {
	if len(selector.Columns) == 0 {
		b.appendViewColumns(sb, aView)
		return nil, nil
	}

	return b.appendSelectorColumns(sb, aView, selector)
}

func (b *Builder) appendSelectorColumns(sb *strings.Builder, aView *view.View, selector *view.Statelet) ([]*view.Column, error) {
	result := make([]*view.Column, 0, len(selector.Columns))
	for i, column := range selector.Columns {
		viewColumn, ok := aView.ColumnByName(column)
		if !ok {
			return nil, fmt.Errorf("not found column %v at view %v", column, aView.Name)
		}

		if i != 0 {
			sb.WriteString(separatorFragment)
		}

		sb.WriteString(" ")
		if aView.Groupable {
			sb.WriteString(groupedProjectionExpression(viewColumn))
		} else {
			sb.WriteString(viewColumn.SqlExpression())
		}
		result = append(result, viewColumn)
	}

	return result, nil
}

func groupedProjectionExpression(column *view.Column) string {
	if column == nil {
		return ""
	}
	expr := column.Name
	if defaultValue := columnDefaultValue(column); defaultValue != "" {
		return "COALESCE(" + expr + "," + defaultValue + ") AS " + column.Name
	}
	return expr
}

func columnDefaultValue(column *view.Column) string {
	if column == nil {
		return ""
	}
	return column.DefaultValue()
}

func (b *Builder) viewAlias(view *view.View) string {
	var alias string
	if view.Alias != "" {
		alias = view.Alias + "."
	}
	return alias
}

func (b *Builder) appendViewColumns(sb *strings.Builder, view *view.View) {
	alias := b.viewAlias(view)

	for i, column := range view.Columns {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}

		sb.WriteString(" ")
		if column.Name == column.SqlExpression() {
			sb.WriteString(alias)
		}

		sb.WriteString(column.SqlExpression())
	}
}

func (b *Builder) rewriteGroupBy(SQL string, allColumns []*view.Column, projectedColumns []*view.Column) (string, error) {
	if len(projectedColumns) == 0 {
		return SQL, nil
	}

	trimmed := strings.TrimSpace(SQL)
	if trimmed == "" {
		return SQL, nil
	}
	wrapped := strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")")
	querySQL := inference.TrimParenthesis(trimmed)
	parsed, err := sqlparser.ParseQuery(querySQL)
	if err != nil || parsed == nil {
		return SQL, err
	}

	selectedPositions := projectedColumnPositions(allColumns, projectedColumns)
	if len(selectedPositions) > 0 {
		items := make(query.List, 0, len(selectedPositions))
		for _, position := range selectedPositions {
			if position <= 0 || position > len(parsed.List) {
				continue
			}
			items = append(items, parsed.List[position-1])
		}
		if len(items) > 0 {
			parsed.List = items
		}
	}

	positions := projectedGroupByPositions(parsed.List, projectedColumns)
	groupBy := make(query.List, 0, len(positions))
	for _, position := range positions {
		groupBy = append(groupBy, query.NewItem(expr.NewIntLiteral(strconv.Itoa(position))))
	}
	parsed.GroupBy = groupBy
	parsed.OrderBy = filterGroupedOrderBy(parsed.OrderBy, parsed.List)

	rewritten := sqlparser.Stringify(parsed)
	if wrapped {
		rewritten = "(" + rewritten + ")"
	}
	return rewritten, nil
}

func projectedColumnPositions(allColumns []*view.Column, projectedColumns []*view.Column) []int {
	index := make(map[*view.Column]int, len(allColumns))
	for i, column := range allColumns {
		index[column] = i + 1
	}
	result := make([]int, 0, len(projectedColumns))
	seen := map[int]bool{}
	for _, column := range projectedColumns {
		if column == nil {
			continue
		}
		position, ok := index[column]
		if !ok || seen[position] {
			continue
		}
		seen[position] = true
		result = append(result, position)
	}
	return result
}

func filterGroupedOrderBy(orderBy query.List, items query.List) query.List {
	if len(orderBy) == 0 || len(items) == 0 {
		return orderBy
	}
	allowed := map[string]bool{}
	for _, item := range items {
		if item == nil {
			continue
		}
		if item.Expr != nil {
			allowed[normalizeExpression(sqlparser.Stringify(item.Expr))] = true
		}
		if item.Alias != "" {
			allowed[normalizeExpression(item.Alias)] = true
		}
	}
	result := make(query.List, 0, len(orderBy))
	for _, item := range orderBy {
		if item == nil || item.Expr == nil {
			continue
		}
		if allowed[normalizeExpression(sqlparser.Stringify(item.Expr))] {
			result = append(result, item)
		}
	}
	return result
}

func normalizeExpression(value string) string {
	return strings.ToUpper(strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
}

func projectedGroupByPositions(items query.List, projectedColumns []*view.Column) []int {
	maxLen := len(items)
	if len(projectedColumns) < maxLen {
		maxLen = len(projectedColumns)
	}
	result := make([]int, 0, maxLen)
	for i := 0; i < maxLen; i++ {
		column := projectedColumns[i]
		if column != nil && column.Groupable {
			result = append(result, i+1)
			continue
		}
		if !isAggregateSelectItem(items[i]) {
			result = append(result, i+1)
		}
	}
	return result
}

func isAggregateSelectItem(item *query.Item) bool {
	if item == nil || item.Expr == nil {
		return false
	}
	return containsAggregateNode(item.Expr)
}

func containsAggregateNode(n node.Node) bool {
	switch actual := n.(type) {
	case nil:
		return false
	case *expr.Call:
		if actual.X != nil {
			switch ident := actual.X.(type) {
			case *expr.Ident:
				if isAggregateFunction(ident.Name) {
					return true
				}
			case *expr.Selector:
				if isAggregateFunction(ident.Name) {
					return true
				}
			}
			if containsAggregateNode(actual.X) {
				return true
			}
		}
		for _, arg := range actual.Args {
			if containsAggregateNode(arg) {
				return true
			}
		}
		return false
	case *expr.Parenthesis:
		return containsAggregateNode(actual.X)
	case *expr.Unary:
		return containsAggregateNode(actual.X)
	case *expr.Binary:
		return containsAggregateNode(actual.X) || containsAggregateNode(actual.Y)
	case *expr.Switch:
		if containsAggregateNode(&actual.Ident) {
			return true
		}
		for _, item := range actual.Cases {
			if item == nil {
				continue
			}
			if containsAggregateNode(item.X) || containsAggregateNode(item.Y) {
				return true
			}
		}
		return false
	case *expr.Ident:
		return isAggregateFunction(actual.Name)
	case *expr.Selector:
		return isAggregateFunction(actual.Name)
	case *expr.Qualify:
		return containsAggregateNode(actual.X)
	}
	return false
}

func isAggregateFunction(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SUM", "COUNT", "AVG", "MIN", "MAX", "ARRAY_AGG", "STRING_AGG", "ANY_VALUE":
		return true
	default:
		return false
	}
}

func (b *Builder) appendViewAlias(sb *strings.Builder, view *view.View) {
	if view.Alias == "" {
		return
	}

	sb.WriteString(asFragment)
	sb.WriteString(view.Alias)
}

func (b *Builder) updatePagination(params *view.CriteriaParam, view *view.View, selector *view.Statelet, exclude *Exclude) error {
	if exclude.Pagination {
		return nil
	}

	sb := strings.Builder{}
	if err := b.appendOrderBy(&sb, view, selector); err != nil {
		return err
	}
	b.appendLimit(&sb, view, selector)
	b.appendOffset(&sb, selector)
	params.Pagination = sb.String()
	return nil
}

func (b *Builder) appendLimit(sb *strings.Builder, aView *view.View, selector *view.Statelet) {
	limit := actualLimit(aView, selector)
	if limit == 0 {
		return
	}

	sb.WriteString(limitFragment)
	sb.WriteString(strconv.Itoa(limit))
}

func (b *Builder) appendOffset(sb *strings.Builder, selector *view.Statelet) {
	if selector.Offset == 0 {
		return
	}

	sb.WriteString(offsetFragment)
	sb.WriteString(strconv.Itoa(selector.Offset))
}

func (b *Builder) updateCriteria(params *view.CriteriaParam, columnsInMeta *reservedMeta) error {
	sb := strings.Builder{}
	hasColumnsIn := columnsInMeta.has()

	if !hasColumnsIn && params.ColumnsIn != "" {
		b.appendCriteria(&sb, keywords.ColumnsIn, false)
	}

	params.WhereClause = sb.String()

	return nil
}

func (b *Builder) appendCriteria(sb *strings.Builder, criteria string, addAnd bool) {
	if addAnd {
		sb.WriteString(andFragment)
	}

	sb.WriteString(criteria)

	if addAnd {
		sb.WriteString(encloseFragment)
	}
}

func (b *Builder) updateColumnsIn(params *view.CriteriaParam, batchData *view.BatchData, exclude *Exclude) {
	if exclude.ColumnsIn {
		return
	}
	if batchData == nil || len(batchData.ColumnNames) == 0 {
		return
	}

	sb := strings.Builder{}
	sb.WriteString(" ")
	columns := len(batchData.ColumnNames)

	switch columns {
	case 1:
		sb.WriteString(batchData.ColumnNames[0])
	default:
		sb.WriteString("(")
		for i, column := range batchData.ColumnNames {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(column)
		}
		sb.WriteString(")")
	}
	sb.WriteString(inFragment)
	for i := 0; i < len(batchData.ValuesBatch); i += columns {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		switch columns {
		case 1:
			sb.WriteString(placeholderFragment)
		default:
			sb.WriteString("(")
			for j := 0; j < columns; j++ {
				if j > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(placeholderFragment)
			}
			sb.WriteString(")")
		}
	}
	sb.WriteString(encloseFragment)
	params.ColumnsIn = sb.String()
}

func (b *Builder) appendOrderBy(sb *strings.Builder, aView *view.View, selector *view.Statelet) error {
	if selector.OrderBy != "" {
		fragment := strings.Builder{}
		items := strings.Split(strings.ReplaceAll(selector.OrderBy, ":", " "), ",")
		for i, item := range items {
			if item == "" {
				continue
			}
			if i > 0 {
				fragment.WriteString(separatorFragment)
			}
			column := item
			sortDirection := ""
			if index := strings.Index(item, " "); index != -1 {
				column = item[:index]
				sortDirection = item[index+1:]
			}

			switch strings.ToLower(sortDirection) {
			case "asc", "desc", "":
			default:
				return fmt.Errorf("invalid sort direction %v for column %v at aView %v", sortDirection, column, aView.Name)
			}

			col, ok := aView.ColumnByName(column)
			if !ok {

				if aView.Selector.Constraints.HasOrderByColumn(column) {
					mapped := aView.Selector.Constraints.OrderByColumn[column]
					col = &view.Column{
						Name: mapped,
					}
					ok = true
				}

			}
			if !ok {
				return fmt.Errorf("not found column %v at aView %v", column, aView.Name)
			}
			fragment.WriteString(col.Name)
			if sortDirection != "" {
				fragment.WriteString(" ")
				fragment.WriteString(sortDirection)
			}
		}
		sb.WriteString(orderByFragment)
		sb.WriteString(fragment.String())
		return nil
	}

	if aView.Selector.OrderBy != "" {
		sb.WriteString(orderByFragment)
		sb.WriteString(strings.ReplaceAll(aView.Selector.OrderBy, ":", " "))
		return nil
	}

	return nil
}

func (b *Builder) appendRelationColumn(sb *strings.Builder, aView *view.View, selector *view.Statelet, relation *view.Relation) error {
	if relation == nil {
		return nil
	}

	if len(selector.Columns) > 0 {
		return b.checkSelectorAndAppendRelColumn(sb, aView, selector, relation)
	}

	return b.checkViewAndAppendRelColumn(sb, aView, relation)
}

func (b *Builder) checkViewAndAppendRelColumn(sb *strings.Builder, aView *view.View, relation *view.Relation) error {
	if _, _, ok := b.lookupRelationColumn(aView, relation); ok {
		return nil
	}

	if aView.Template.IsActualTemplate() {
		return nil
	}

	sb.WriteString(separatorFragment)
	sb.WriteString(aView.Alias)
	sb.WriteString(".")
	sb.WriteString(relation.Of.On[0].Column)
	sb.WriteString(" ")

	return nil
}

func (b *Builder) checkSelectorAndAppendRelColumn(sb *strings.Builder, aView *view.View, selector *view.Statelet, relation *view.Relation) error {
	if relation == nil || aView.Template.IsActualTemplate() {
		return nil
	}
	if b.selectorHasRelationColumn(selector, aView, relation) {
		return nil
	}

	sb.WriteString(separatorFragment)
	sb.WriteString(" ")
	col, _, ok := b.lookupRelationColumn(aView, relation)
	if !ok {
		sb.WriteString(relation.Of.On[0].Column)
	} else {
		sb.WriteString(col.SqlExpression())
	}

	return nil
}

func (b *Builder) selectorHasRelationColumn(selector *view.Statelet, aView *view.View, relation *view.Relation) bool {
	if selector == nil || relation == nil || relation.Of == nil || len(relation.Of.On) == 0 {
		return false
	}
	link := relation.Of.On[0]
	if selector.Has(link.Column) {
		return true
	}
	if link.Field != "" && selector.Has(link.Field) {
		return true
	}
	if column, _, ok := b.lookupRelationColumn(aView, relation); ok {
		if selector.Has(column.Name) {
			return true
		}
		if field := column.Field(); field != nil && selector.Has(field.Name) {
			return true
		}
	}
	return false
}

func (b *Builder) lookupRelationColumn(aView *view.View, relation *view.Relation) (*view.Column, string, bool) {
	if aView == nil || relation == nil || relation.Of == nil || len(relation.Of.On) == 0 {
		return nil, "", false
	}
	link := relation.Of.On[0]
	if link.Field != "" {
		if column, ok := aView.ColumnByName(link.Field); ok {
			return column, link.Field, true
		}
	}
	if link.Column != "" {
		if column, ok := aView.ColumnByName(link.Column); ok {
			return column, link.Column, true
		}
	}
	return nil, "", false
}

func actualLimit(aView *view.View, selector *view.Statelet) int {
	if selector.Limit != 0 {
		return selector.Limit
	}

	return aView.Selector.Limit
}

func (b *Builder) ExactMetaSQL(ctx context.Context, aView *view.View, selector *view.Statelet, batchData *view.BatchData, relation *view.Relation, parent *expand.ViewContext) (*cache.ParmetrizedQuery, error) {
	return b.metaSQL(ctx, aView, selector, batchData, relation, &Exclude{
		Pagination: true,
	}, parent, nil)
}

func (b *Builder) CacheMetaSQL(ctx context.Context, aView *view.View, selector *view.Statelet, batchData *view.BatchData, relation *view.Relation, parent *expand.ViewContext) (*cache.ParmetrizedQuery, error) {
	return b.metaSQL(ctx, aView, selector, batchData, relation, &Exclude{Pagination: true, ColumnsIn: true}, parent, &expand.MockExpander{})
}

func (b *Builder) CacheSQL(ctx context.Context, aView *view.View, selector *view.Statelet) (*cache.ParmetrizedQuery, error) {
	return b.CacheSQLWithOptions(ctx, aView, selector, nil, nil, nil)
}

func (b *Builder) CacheSQLWithOptions(ctx context.Context, aView *view.View, statelet *view.Statelet, batchData *view.BatchData, relation *view.Relation, parent *expand.ViewContext) (*cache.ParmetrizedQuery, error) {
	return b.Build(ctx, WithBuilderView(aView),
		WithBuilderStatelet(statelet),
		WithBuilderBatchData(batchData),
		WithBuilderRelation(relation),
		WithBuilderExclude(true, true))
}

func (b *Builder) metaSQL(ctx context.Context, aView *view.View, statelet *view.Statelet, batchData *view.BatchData, relation *view.Relation, exclude *Exclude, parent *expand.ViewContext, expander expand.Expander) (*cache.ParmetrizedQuery, error) {
	matcher, err := b.Build(ctx, WithBuilderView(aView), WithBuilderStatelet(statelet), WithBuilderBatchData(batchData), WithBuilderRelation(relation), WithBuilderExclude(exclude.ColumnsIn, exclude.Pagination), WithBuilderParent(parent), WithBuilderExpander(expander))
	if err != nil {
		return nil, err
	}

	viewParam := view.AsViewParam(aView, statelet, batchData)
	viewParam.NonWindowSQL = matcher.SQL
	viewParam.Args = matcher.Args

	SQL, args, err := aView.Template.Summary.Evaluate(ctx, statelet.Template, viewParam)
	if err != nil {
		return nil, err
	}

	if len(args) == 0 {
		args = matcher.Args
	}

	matcher.SQL = SQL
	if len(args) > 0 {
		matcher.Args = args
	}
	if relation != nil {
		matcher.By = relation.Of.On[0].Field
	}
	return matcher, nil
}
