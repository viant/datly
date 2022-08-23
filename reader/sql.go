package reader

import (
	"fmt"
	"github.com/viant/datly/reader/metadata"
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
	Exclude struct {
		ColumnsIn  bool
		Pagination bool
	}

	//BatchData groups view needed to use various view.MatchStrategy
)

//NewBuilder creates Builder instance
func NewBuilder() *Builder {
	return &Builder{}
}

//Build builds SQL Select statement
func (b *Builder) Build(aView *view.View, selector *view.Selector, batchData *view.BatchData, relation *view.Relation, exclude *Exclude, parent *view.MetaParam) (*cache.Matcher, error) {
	if exclude == nil {
		exclude = &Exclude{}
	}

	template, sanitized, _, err := aView.Template.EvaluateSource(selector.Parameters.Values, selector.Parameters.Has, parent)
	if err != nil {
		return nil, err
	}

	if aView.Template.IsActualTemplate() && aView.ShouldTryDiscover() {
		template = metadata.EnrichWithDiscover(template, true)
	}

	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	if err = b.appendColumns(&sb, aView, selector); err != nil {
		return nil, err
	}

	if err = b.appendRelationColumn(&sb, aView, selector, relation); err != nil {
		return nil, err
	}

	sb.WriteString(fromFragment)
	sb.WriteString(template)
	b.appendViewAlias(&sb, aView)

	columnsInMeta := hasKeyword(template, keywords.ColumnsIn)
	commonParams := view.CriteriaParam{}

	criteriaMeta := hasKeyword(template, keywords.Criteria)
	hasCriteria := criteriaMeta.has()

	b.updateColumnsIn(&commonParams, aView, relation, batchData, columnsInMeta, hasCriteria, exclude)

	if err = b.updatePagination(&commonParams, aView, selector, exclude); err != nil {
		return nil, err
	}

	if err = b.updateCriteria(&commonParams, columnsInMeta); err != nil {
		return nil, err
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

	hasPagination := strings.Contains(template, keywords.Pagination)
	if !hasPagination && commonParams.Pagination != "" {
		sb.WriteString(" ")
		sb.WriteString(keywords.Pagination)
		sb.WriteString(" ")
	}

	var placeholders []interface{}

	SQL, err := aView.Expand(&placeholders, sb.String(), selector, commonParams, batchData, sanitized)
	if err != nil {
		return nil, err
	}

	matcher := &cache.Matcher{
		SQL:  SQL,
		Args: placeholders,
	}

	if exclude.ColumnsIn && relation != nil {
		matcher.IndexBy = relation.Of.Column
		matcher.In = batchData.ValuesBatch
	}

	if exclude.Pagination {
		matcher.Offset = selector.Offset
		matcher.Limit = actualLimit(aView, selector)
	}

	return matcher, err
}

func (b *Builder) appendColumns(sb *strings.Builder, aView *view.View, selector *view.Selector) error {
	if len(selector.Columns) == 0 {
		b.appendViewColumns(sb, aView)
		return nil
	}

	return b.appendSelectorColumns(sb, aView, selector)
}

func (b *Builder) appendSelectorColumns(sb *strings.Builder, view *view.View, selector *view.Selector) error {
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

func (b *Builder) updatePagination(params *view.CriteriaParam, view *view.View, selector *view.Selector, exclude *Exclude) error {
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

func (b *Builder) appendLimit(sb *strings.Builder, aView *view.View, selector *view.Selector) {
	limit := actualLimit(aView, selector)
	if limit == 0 {
		return
	}

	sb.WriteString(limitFragment)
	sb.WriteString(strconv.Itoa(limit))
}

func (b *Builder) appendOffset(sb *strings.Builder, selector *view.Selector) {
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

func (b *Builder) updateColumnsIn(params *view.CriteriaParam, view *view.View, relation *view.Relation, batchData *view.BatchData, columnsInMeta *reservedMeta, hasCriteria bool, exclude *Exclude) {
	if exclude.ColumnsIn {
		return
	}

	columnsIn := columnsInMeta.has()

	if batchData == nil || batchData.ColumnName == "" {
		return
	}

	alias := b.viewAlias(view)
	if hasCriteria || columnsIn {
		if relation.ColumnAlias != "" {
			alias = relation.ColumnAlias + "."
		} else {
			alias = ""
		}
	}

	sb := strings.Builder{}
	sb.WriteString(" ")
	sb.WriteString(alias)
	sb.WriteString(batchData.ColumnName)
	sb.WriteString(inFragment)

	for i := range batchData.ValuesBatch {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		sb.WriteString(placeholderFragment)
	}
	sb.WriteString(encloseFragment)
	params.ColumnsIn = sb.String()
}

func (b *Builder) appendOrderBy(sb *strings.Builder, view *view.View, selector *view.Selector) error {
	if selector.OrderBy != "" {
		col, ok := view.ColumnByName(selector.OrderBy)
		if !ok {
			return fmt.Errorf("not found column %v at view %v", selector.OrderBy, view.Name)
		}

		sb.WriteString(orderByFragment)
		sb.WriteString(col.Name)
		return nil
	}

	if view.Selector.OrderBy != "" {
		sb.WriteString(orderByFragment)
		sb.WriteString(view.Selector.OrderBy)
		return nil
	}

	return nil
}

func (b *Builder) appendRelationColumn(sb *strings.Builder, aView *view.View, selector *view.Selector, relation *view.Relation) error {
	if relation == nil {
		return nil
	}

	if len(selector.Columns) > 0 {
		return b.checkSelectorAndAppendRelColumn(sb, aView, selector, relation)
	}

	return b.checkViewAndAppendRelColumn(sb, aView, relation)
}

func (b *Builder) checkViewAndAppendRelColumn(sb *strings.Builder, aView *view.View, relation *view.Relation) error {
	if _, ok := aView.ColumnByName(relation.Of.Column); ok {
		return nil
	}

	if aView.Template.IsActualTemplate() {
		return nil
	}

	sb.WriteString(separatorFragment)
	sb.WriteString(aView.Alias)
	sb.WriteString(".")
	sb.WriteString(relation.Of.Column)
	sb.WriteString(" ")

	return nil
}

func (b *Builder) checkSelectorAndAppendRelColumn(sb *strings.Builder, aView *view.View, selector *view.Selector, relation *view.Relation) error {
	if relation == nil || selector.Has(relation.Of.Column) || aView.Template.IsActualTemplate() {
		return nil
	}

	sb.WriteString(separatorFragment)
	sb.WriteString(" ")
	col, ok := aView.ColumnByName(relation.Of.Column)
	if !ok {
		sb.WriteString(relation.Of.Column)
	} else {
		sb.WriteString(col.SqlExpression())
	}

	return nil
}

func actualLimit(aView *view.View, selector *view.Selector) int {
	if selector.Limit != 0 {
		return selector.Limit
	}

	return aView.Selector.Limit
}
