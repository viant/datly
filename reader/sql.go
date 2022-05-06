package reader

import (
	"fmt"
	"github.com/viant/datly/data"
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

	//BatchData groups data needed to use various data.MatchStrategy
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
func (b *Builder) Build(view *data.View, selector *data.Selector, batchData *BatchData, relation *data.Relation, parent *data.View) (string, []interface{}, error) {
	template, err := view.Template.EvaluateSource(selector.Parameters.Values, selector.Parameters.Has, parent)
	if err != nil {
		return "", nil, err
	}

	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	if err = b.appendColumns(&sb, view, selector); err != nil {
		return "", nil, err
	}

	sb.WriteString(fromFragment)
	sb.WriteString(template)
	b.appendViewAlias(&sb, view)

	hasColumnsIn := strings.Contains(template, data.ColumnsIn)
	commonParams := data.CommonParams{}

	b.updateColumnsIn(&commonParams, view, relation, batchData, hasColumnsIn)

	hasCriteria := strings.Contains(template, data.Criteria)

	if err = b.updatePagination(&commonParams, view, selector); err != nil {
		return "", nil, err
	}

	if err = b.updateCriteria(&commonParams, view, selector, hasColumnsIn, parent); err != nil {
		return "", nil, err
	}

	if !hasCriteria {
		sb.WriteString(" ")
		sb.WriteString(data.Criteria)
		sb.WriteString(" ")
	}

	hasPagination := strings.Contains(template, data.Pagination)
	if !hasPagination {
		sb.WriteString(" ")
		sb.WriteString(data.Pagination)
		sb.WriteString(" ")
	}

	return b.expand(sb.String(), view, selector, commonParams, batchData)
}

func (b *Builder) appendColumns(sb *strings.Builder, view *data.View, selector *data.Selector) error {
	if len(selector.Columns) == 0 {
		b.appendViewColumns(sb, view)
		return nil
	}

	return b.appendSelectorColumns(sb, view, selector)
}

func (b *Builder) appendSelectorColumns(sb *strings.Builder, view *data.View, selector *data.Selector) error {
	alias := b.viewAlias(view)

	for i, column := range selector.Columns {
		viewColumn, ok := view.ColumnByName(column)
		if !ok {
			return fmt.Errorf("not found column %v at view %v", column, view.Name)
		}

		if i != 0 {
			sb.WriteString(separatorFragment)
		}

		sb.WriteString(" ")
		if viewColumn.SqlExpression() == view.Name {
			sb.WriteString(alias)
		}
		sb.WriteString(viewColumn.SqlExpression())
	}

	return nil
}

func (b *Builder) viewAlias(view *data.View) string {
	var alias string
	if view.Alias != "" {
		alias = view.Alias + "."
	}
	return alias
}

func (b *Builder) appendViewColumns(sb *strings.Builder, view *data.View) {
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

func (b *Builder) appendViewAlias(sb *strings.Builder, view *data.View) {
	if view.Alias == "" {
		return
	}

	sb.WriteString(asFragment)
	sb.WriteString(view.Alias)
}

func (b *Builder) updatePagination(params *data.CommonParams, view *data.View, selector *data.Selector) error {
	sb := strings.Builder{}
	if err := b.appendOrderBy(&sb, view, selector); err != nil {
		return err
	}
	b.appendLimit(&sb, view, selector)
	b.appendOffset(&sb, selector)
	params.Pagination = sb.String()
	return nil
}

func (b *Builder) appendLimit(sb *strings.Builder, view *data.View, selector *data.Selector) {
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

func (b *Builder) appendOffset(sb *strings.Builder, selector *data.Selector) {
	if selector.Offset == 0 {
		return
	}

	sb.WriteString(offsetFragment)
	sb.WriteString(strconv.Itoa(selector.Offset))
}

func (b *Builder) expand(sql string, view *data.View, selector *data.Selector, params data.CommonParams, batchData *BatchData) (string, []interface{}, error) {
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
			case data.Pagination[1:]:
				replacement.SetValue(key, params.Pagination)
			case data.Criteria[1:]:
				criteriaExpanded, criteriaPlaceholders, err := b.expand(params.WhereClause, view, selector, params, batchData)
				if err != nil {
					return "", nil, err
				}
				replacement.SetValue(key, criteriaExpanded)
				placeholders = append(placeholders, criteriaPlaceholders...)
			case data.ColumnsIn[1:]:
				replacement.SetValue(key, params.ColumnsIn)
				placeholders = append(placeholders, batchData.ValuesBatch...)

			default:
				replacement.SetValue(key, `?`)
				accessor, err := view.Template.AccessorByName(key)
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

func (b *Builder) updateCriteria(params *data.CommonParams, view *data.View, selector *data.Selector, hasColumnsIn bool, parent *data.View) error {
	sb := strings.Builder{}
	addAnd := false
	if !hasColumnsIn && params.ColumnsIn != "" {
		b.appendCriteria(&sb, data.ColumnsIn, false)
		addAnd = true
	}

	if view.Criteria != "" {
		criteria, err := b.viewCriteria(view, selector, parent)
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

func (b *Builder) viewCriteria(view *data.View, selector *data.Selector, parent *data.View) (string, error) {
	criteria, err := view.Template.EvaluateCriteria(selector.Parameters.Values, selector.Parameters.Has, parent)
	if err != nil {
		return "", err
	}

	return criteria, nil
}

func (b *Builder) updateColumnsIn(params *data.CommonParams, view *data.View, relation *data.Relation, batchData *BatchData, hasColumnsIn bool) {
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

func (b *Builder) appendOrderBy(sb *strings.Builder, view *data.View, selector *data.Selector) error {
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
