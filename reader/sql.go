package reader

import (
	"fmt"
	"github.com/viant/datly/view"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
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
	whereFragment       = " WHERE "
	inFragment          = " IN ("
	andFragment         = " AND ("
	placeholderFragment = "?"
	encloseFragment     = ")"
)

type (

	//Builder represent SQL Builder
	Builder struct{}

	//BatchData groups view needed to use various view.MatchStrategy
	BatchData struct {
		ColumnName     string
		Parent         int
		ParentReadSize int

		Values      []interface{}
		ValuesBatch []interface{}
	}
)

//NewBuilder creates Builder instance
func NewBuilder() *Builder {
	return &Builder{}
}

//Build builds SQL Select statement
func (b *Builder) Build(aView *view.View, selector *view.Selector, batchData *BatchData, relation *view.Relation, parent *view.View) (string, []interface{}, error) {
	template, err := aView.Template.EvaluateSource(selector.Parameters.Values, selector.Parameters.Has, parent)
	if err != nil {
		return "", nil, err
	}

	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	if err = b.appendColumns(&sb, aView, selector, relation); err != nil {
		return "", nil, err
	}

	sb.WriteString(fromFragment)
	sb.WriteString(template)
	b.appendViewAlias(&sb, aView)

	hasColumnsIn := strings.Contains(template, view.ColumnsIn)
	commonParams := view.CommonParams{}

	b.updateColumnsIn(&commonParams, aView, relation, batchData, hasColumnsIn)

	hasCriteria := strings.Contains(template, view.Criteria)

	if err = b.updatePagination(&commonParams, aView, selector); err != nil {
		return "", nil, err
	}

	if err = b.updateCriteria(&commonParams, aView, selector, hasColumnsIn, parent); err != nil {
		return "", nil, err
	}

	if !hasCriteria {
		sb.WriteString(" ")
		sb.WriteString(view.Criteria)
		sb.WriteString(" ")
	}

	hasPagination := strings.Contains(template, view.Pagination)
	if !hasPagination {
		sb.WriteString(" ")
		sb.WriteString(view.Pagination)
		sb.WriteString(" ")
	}

	return b.expand(sb.String(), aView, selector, commonParams, batchData)
}

func (b *Builder) appendColumns(sb *strings.Builder, aView *view.View, selector *view.Selector, relation *view.Relation) error {
	if len(selector.Columns) == 0 {
		b.appendViewColumns(sb, aView)
		return nil
	}

	return b.appendSelectorColumns(sb, aView, selector, relation)
}

func (b *Builder) appendSelectorColumns(sb *strings.Builder, view *view.View, selector *view.Selector, relation *view.Relation) error {
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

	if relation != nil && !selector.Has(relation.Of.Column) {
		sb.WriteString(separatorFragment)
		sb.WriteString(" ")
		col, ok := view.ColumnByName(relation.Of.Column)
		if !ok {
			return fmt.Errorf("not found column %v at view %v", relation.Of.Column, view.Name)
		}
		sb.WriteString(col.SqlExpression())
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

func (b *Builder) updatePagination(params *view.CommonParams, view *view.View, selector *view.Selector) error {
	sb := strings.Builder{}
	if err := b.appendOrderBy(&sb, view, selector); err != nil {
		return err
	}
	b.appendLimit(&sb, view, selector)
	b.appendOffset(&sb, selector)
	params.Pagination = sb.String()
	return nil
}

func (b *Builder) appendLimit(sb *strings.Builder, view *view.View, selector *view.Selector) {
	if selector.Limit != 0 && (selector.Limit < view.Selector.Limit || view.Selector.Limit == 0) {
		sb.WriteString(limitFragment)
		sb.WriteString(strconv.Itoa(selector.Limit))
		return
	}

	if view.Selector.Limit != 0 {
		sb.WriteString(limitFragment)
		sb.WriteString(strconv.Itoa(view.Selector.Limit))
		return
	}
}

func (b *Builder) appendOffset(sb *strings.Builder, selector *view.Selector) {
	if selector.Offset == 0 {
		return
	}

	sb.WriteString(offsetFragment)
	sb.WriteString(strconv.Itoa(selector.Offset))
}

func (b *Builder) expand(sql string, aView *view.View, selector *view.Selector, params view.CommonParams, batchData *BatchData) (string, []interface{}, error) {
	placeholders := make([]interface{}, 0)
	block, err := parser.Parse([]byte(sql))
	if err != nil {
		return "", nil, err
	}

	replacement := rdata.Map{}

	for _, statement := range block.Stmt {
		switch actual := statement.(type) {
		case *expr.Select:
			key := extractSelectorName(actual.FullName)
			switch key {
			case view.Pagination[1:]:
				replacement.SetValue(key, params.Pagination)
			case view.Criteria[1:]:
				criteriaExpanded, criteriaPlaceholders, err := b.expand(params.WhereClause, aView, selector, params, batchData)
				if err != nil {
					return "", nil, err
				}
				replacement.SetValue(key, criteriaExpanded)
				placeholders = append(placeholders, criteriaPlaceholders...)
			case view.ColumnsIn[1:]:
				replacement.SetValue(key, params.ColumnsIn)
				placeholders = append(placeholders, batchData.ValuesBatch...)

			default:
				replacement.SetValue(key, `?`)
				accessor, err := aView.Template.AccessorByName(key)
				if err != nil {
					return "", nil, err
				}
				value, err := accessor.Value(selector.Parameters.Values)
				if err != nil {
					return "", nil, err
				}
				placeholders = append(placeholders, value)
			}
		}
	}

	return replacement.ExpandAsText(sql), placeholders, err
}

func (b *Builder) updateCriteria(params *view.CommonParams, aView *view.View, selector *view.Selector, hasColumnsIn bool, parent *view.View) error {
	sb := strings.Builder{}
	addAnd := false
	if !hasColumnsIn && params.ColumnsIn != "" {
		b.appendCriteria(&sb, view.ColumnsIn, false)
		addAnd = true
	}

	if aView.Criteria != "" {
		criteria, err := b.viewCriteria(aView, selector, parent)
		if err != nil {
			return err
		}

		b.appendCriteria(&sb, criteria, addAnd)
		addAnd = true
	}

	if selector.Criteria != "" {
		b.appendCriteria(&sb, selector.Criteria, addAnd)
		addAnd = true
	}

	params.WhereClause = sb.String()
	return nil
}

func (b *Builder) appendCriteria(sb *strings.Builder, criteria string, addAnd bool) {
	if addAnd {
		sb.WriteString(andFragment)
	} else {
		sb.WriteString(whereFragment)
	}

	sb.WriteString(criteria)

	if addAnd {
		sb.WriteString(encloseFragment)
	}
}

func (b *Builder) viewCriteria(view *view.View, selector *view.Selector, parent *view.View) (string, error) {
	criteria, err := view.Template.EvaluateCriteria(selector.Parameters.Values, selector.Parameters.Has, parent)
	if err != nil {
		return "", err
	}

	return criteria, nil
}

func (b *Builder) updateColumnsIn(params *view.CommonParams, view *view.View, relation *view.Relation, batchData *BatchData, hasColumnsIn bool) {
	if batchData == nil || batchData.ColumnName == "" {
		return
	}

	alias := b.viewAlias(view)
	if hasColumnsIn && relation.ColumnAlias != "" {
		alias = relation.ColumnAlias + "."
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

func extractSelectorName(name string) string {
	i := 1 // all names starts with the '$'

	for ; i < len(name) && name[i] == '{'; i++ {
	} // skip the select block i.e. ${foo.Name}

	return name[i : len(name)-i+1]
}
