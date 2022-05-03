package reader

import (
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/shared"
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
func (b *Builder) Build(view *data.View, selector *data.Selector, batchData *BatchData, relation *data.Relation) (string, error) {
	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	alias := view.Alias

	var err error
	if err = b.appendColumns(view, selector, &sb); err != nil {
		return "", err
	}

	sb.WriteString(fromFragment)
	if err = b.appendSource(&sb, view, selector, batchData, relation); err != nil {
		return "", err
	}

	if !view.HasColumnInReplacement() && !view.HasCriteriaReplacement() {
		whereClause, err := b.buildWhereClause(view, true, selector, alias, batchData)
		if err != nil {
			return "", err
		}
		b.appendWhereClause(whereClause, &sb)
	} else if view.HasColumnInReplacement() {
		whereClause, err := b.buildWhereClause(view, false, selector, alias, batchData)
		if err != nil {
			return "", err
		}
		b.appendWhereClause(whereClause, &sb)
	}

	if err = b.appendOrderBy(view, selector, &sb); err != nil {
		return "", err
	}

	if !view.HasPaginationReplacement() {
		b.appendLimit(view, selector, batchData, &sb)
		b.appendOffset(view, selector, batchData, &sb)
	}

	result := sb.String()
	if view.HasPaginationReplacement() {
		paginationSb := strings.Builder{}
		b.appendLimit(view, selector, batchData, &paginationSb)
		b.appendOffset(view, selector, batchData, &paginationSb)
		result = strings.ReplaceAll(result, string(shared.Pagination), paginationSb.String())
	}

	return result, nil
}

func (b *Builder) appendWhereClause(whereClause string, sb *strings.Builder) {
	if whereClause == "" {
		return
	}

	sb.WriteString(whereFragment)
	sb.WriteString(whereClause)
}

func (b *Builder) appendColumns(view *data.View, selector *data.Selector, sb *strings.Builder) error {
	columns, err := view.SelectedColumns(selector)
	if err != nil {
		return err
	}
	for i, col := range columns {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}

		if col.SqlExpression() != col.Name {
			sb.WriteString(col.SqlExpression())
		} else {
			if view.Alias != "" {
				sb.WriteString(view.Alias)
				sb.WriteString(".")
			}
			sb.WriteString(col.Name)
		}
	}
	return nil
}

func (b *Builder) appendOrderBy(view *data.View, selector *data.Selector, sb *strings.Builder) error {
	orderBy := view.Selector.OrderBy
	if view.CanUseSelectorOrderBy() && selector != nil && selector.OrderBy != "" {
		orderBy = selector.OrderBy
	}

	if orderBy != "" {
		if _, ok := view.ColumnByName(orderBy); !ok {
			return fmt.Errorf("invalid orderBy column: %v", orderBy)
		}
		sb.WriteString(orderByFragment)
		sb.WriteString(orderBy)
	}
	return nil
}

func (b *Builder) appendLimit(view *data.View, selector *data.Selector, batchData *BatchData, sb *strings.Builder) {
	limit := view.LimitWithSelector(selector)
	if limit > 0 {
		sb.WriteString(limitFragment)
		sb.WriteString(strconv.Itoa(limit))
	}
}

func (b *Builder) appendOffset(view *data.View, selector *data.Selector, batchData *BatchData, sb *strings.Builder) {
	offset := 0
	if selector != nil {

		if view.CanUseSelectorOffset() && selector.Offset > 0 {
			offset = selector.Offset
		}
	}

	if offset > 0 {
		sb.WriteString(offsetFragment)
		sb.WriteString(strconv.Itoa(offset))
	}
}

func (b *Builder) buildColumnsIn(batchData *BatchData, alias string) string {
	if batchData.ColumnName == "" {
		return ""
	}

	sb := strings.Builder{}

	if alias != "" {
		sb.WriteString(alias)
		sb.WriteString(".")
	}

	sb.WriteString(batchData.ColumnName)
	sb.WriteString(inFragment)
	for i := range batchData.ValuesBatch {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		sb.WriteString(placeholderFragment)
	}
	sb.WriteString(") ")
	return sb.String()
}

func (b *Builder) appendSource(sb *strings.Builder, view *data.View, selector *data.Selector, batchData *BatchData, relation *data.Relation) error {
	var alias string
	if relation != nil && relation.ColumnAlias != "" {
		alias = relation.ColumnAlias
	}

	err := b.evaluateAndAppendSource(sb, view, selector, batchData, alias)
	if err != nil {
		return err
	}

	if view.Alias != "" {
		sb.WriteString(asFragment)
		sb.WriteString(view.Alias)
		sb.WriteString(" ")
	}
	return nil
}

func (b *Builder) evaluateAndAppendSource(sb *strings.Builder, view *data.View, selector *data.Selector, batchData *BatchData, alias string) error {
	params := data.CommonParams{}
	if view.HasCriteriaReplacement() {
		whereClause, err := b.buildWhereClause(view, true, selector, alias, batchData)
		if err != nil {
			return err
		}
		hasWhere := view.HasWhereClause()

		if whereClause != "" {
			if !hasWhere {
				whereClause = whereFragment + whereClause
			} else {
				whereClause = andFragment + whereClause + encloseFragment + " "
			}
		}
		params.WhereClause = whereClause
	}

	if view.HasColumnInReplacement() {
		params.ColumnsIn = b.buildColumnsIn(batchData, alias)
	}

	source, err := view.Template.EvaluateSource(params, selector.Parameters.Values, selector.Parameters.Has)
	if err != nil {
		return err
	}

	sb.WriteString(source)
	return nil
}

func (b *Builder) buildWhereClause(view *data.View, useColumnsIn bool, selector *data.Selector, alias string, batchData *BatchData) (string, error) {
	sb := strings.Builder{}

	addAnd := false
	columnsIn := b.buildColumnsIn(batchData, alias)
	if columnsIn != "" && useColumnsIn {
		sb.WriteString(columnsIn)
		addAnd = true
	}

	if view.Criteria != nil && view.Criteria.Expression != "" {
		if addAnd {
			sb.WriteString(andFragment)
		}

		criteria, err := b.viewCriteria(view, selector)
		if err != nil {
			return "", err
		}

		sb.WriteString(criteria)
		if addAnd {
			sb.WriteString(encloseFragment)
		}

		addAnd = true
	}

	if selector != nil && selector.Criteria != "" {
		if addAnd {
			sb.WriteString(andFragment)
		}

		sb.WriteString(selector.Criteria)

		if addAnd {
			sb.WriteString(encloseFragment)
		}

		addAnd = true
	}

	return sb.String(), nil
}

func (b *Builder) viewCriteria(view *data.View, selector *data.Selector) (string, error) {
	if selector == nil {
		return view.Criteria.Expression, nil
	}

	criteria, err := view.Template.EvaluateCriteria(selector.Parameters.Values, selector.Parameters.Has)
	if err != nil {
		return "", err
	}
	return criteria, nil
}
