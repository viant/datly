package reader

import (
	"context"
	"fmt"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/reader/metadata"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/sqlx/io/read/cache"
	"strconv"
	"strings"
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
	batchData := *options.batchData
	relation := options.relation
	exclude := options.exclude
	parent := options.parent
	partitions := options.partition
	expander := options.expander
	state, err := aView.Template.EvaluateSource(ctx, statelet.Template, parent, &batchData, expander)

	if err != nil {
		return nil, err
	}
	if len(state.Filters) > 0 {
		statelet.Filters = append(statelet.Filters, state.Filters...)
	}
	if aView.Template.IsActualTemplate() && aView.ShouldTryDiscover() {
		state.Expanded = metadata.EnrichWithDiscover(state.Expanded, true)
	}

	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	if err = b.appendColumns(&sb, aView, statelet); err != nil {
		return nil, err
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

func (b *Builder) appendColumns(sb *strings.Builder, aView *view.View, selector *view.Statelet) error {
	if len(selector.Columns) == 0 {
		b.appendViewColumns(sb, aView)
		return nil
	}

	return b.appendSelectorColumns(sb, aView, selector)
}

func (b *Builder) appendSelectorColumns(sb *strings.Builder, view *view.View, selector *view.Statelet) error {
	for i, column := range selector.Columns {
		viewColumn, ok := view.ColumnByName(column)
		if !ok {
			return fmt.Errorf("not found column %v at view %v", column, view.Name)
		}

		if i != 0 {
			sb.WriteString(separatorFragment)
		}

		sb.WriteString(" ")
		sb.WriteString(viewColumn.SqlExpression())
	}

	return nil
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

func (b *Builder) appendOrderBy(sb *strings.Builder, view *view.View, selector *view.Statelet) error {
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
			col, ok := view.ColumnByName(column)
			if !ok {
				return fmt.Errorf("not found column %v at view %v", column, view.Name)
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

	if view.Selector.OrderBy != "" {
		sb.WriteString(orderByFragment)
		sb.WriteString(strings.ReplaceAll(view.Selector.OrderBy, ":", " "))
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
	if _, ok := aView.ColumnByName(relation.Of.On[0].Column); ok {
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
	if relation == nil || selector.Has(relation.Of.On[0].Column) || aView.Template.IsActualTemplate() {
		return nil
	}

	sb.WriteString(separatorFragment)
	sb.WriteString(" ")
	col, ok := aView.ColumnByName(relation.Of.On[0].Column)
	if !ok {
		sb.WriteString(relation.Of.On[0].Column)
	} else {
		sb.WriteString(col.SqlExpression())
	}

	return nil
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
