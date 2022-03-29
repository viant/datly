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
		BatchReadSize int
		Read          int
		ColumnName    string
		Values        []interface{}
	}
)

//NewBuilder creates Builder instance
func NewBuilder() *Builder {
	return &Builder{}
}

//Build builds SQL Select statement
func (b *Builder) Build(view *data.View, selector *data.Selector, batchData *BatchData) (string, error) {
	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	alias := view.AliasWith(selector)

	var err error
	if err = b.appendColumns(view, selector, &sb); err != nil {
		return "", err
	}

	sb.WriteString(fromFragment)
	b.appendSource(&sb, view, selector, batchData)

	if !view.HasColumnInReplacement() && !view.HasCriteriaReplacement() {
		whereClause := b.buildWhereClause(view, true, selector, alias, batchData)
		b.appendWhereClause(whereClause, &sb)
	} else if view.HasColumnInReplacement() {
		whereClause := b.buildWhereClause(view, false, selector, alias, batchData)
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

		if col.Expression != "" {
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
	if limit == 0 {
		limit = batchData.BatchReadSize
	} else if limit != 0 {
		toRead := limit - batchData.BatchReadSize - batchData.Read
		if toRead >= 0 {
			limit = batchData.BatchReadSize
		} else if toRead < 0 {
			limit = batchData.BatchReadSize + toRead
		}
	}

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

	offset += batchData.Read
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
	for i := range batchData.Values {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		sb.WriteString(placeholderFragment)
	}
	sb.WriteString(") ")
	return sb.String()
}

func (b *Builder) appendSource(sb *strings.Builder, view *data.View, selector *data.Selector, batchData *BatchData) {
	var alias string
	if view.CanUseSelectorAlias() && selector != nil && selector.Alias != "" {
		alias = selector.Alias
	}

	if view.HasCriteriaReplacement() {
		whereClause := b.buildWhereClause(view, true, selector, alias, batchData)
		hasWhere := view.HasWhereClause()

		if whereClause != "" {
			if !hasWhere {
				whereClause = whereFragment + whereClause
			} else {
				whereClause = andFragment + whereClause + encloseFragment + " "
			}
		}

		whereBuilder := strings.Builder{}
		whereBuilder.WriteString(whereClause)

		sb.WriteString(strings.ReplaceAll(view.Source(), string(shared.Criteria), whereBuilder.String()))
	} else if view.HasColumnInReplacement() {
		sb.WriteString(strings.ReplaceAll(view.Source(), string(shared.ColumnInPosition), b.buildColumnsIn(batchData, alias)))
	} else {
		sb.WriteString(view.Source())
	}

	if view.Alias != "" {
		sb.WriteString(asFragment)
		sb.WriteString(view.Alias)
		sb.WriteString(" ")
	}
}

func (b *Builder) buildWhereClause(view *data.View, useColumnsIn bool, selector *data.Selector, alias string, batchData *BatchData) string {
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

		sb.WriteString(view.Criteria.Expression)
		if addAnd {
			sb.WriteString(encloseFragment)
		}

		addAnd = true
	}

	if view.CanUseSelectorCriteria() && selector != nil && selector.Criteria != nil && selector.Criteria.Expression != "" {
		if addAnd {
			sb.WriteString(andFragment)
		}

		sb.WriteString(selector.Criteria.Expression)

		if addAnd {
			sb.WriteString(encloseFragment)
		}

		addAnd = true
	}

	return sb.String()
}
