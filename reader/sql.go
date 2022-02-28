package reader

import (
	"fmt"
	"github.com/viant/datly/data"
	"strconv"
	"strings"
)

const (
	selectFragment      = "SELECT "
	separatorFragment   = ", "
	fromFragment        = " FROM ("
	fromEncloseFragment = ")"
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
		CurrentlyRead int
		ColumnName    string
		Placeholders  []interface{}
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

	columns, err := view.SelectedColumns(selector)
	if err != nil {
		return "", err
	}
	for i, col := range columns {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		sb.WriteString(col.SqlExpression())
	}
	sb.WriteString(fromFragment)
	sb.WriteString(view.Source())
	sb.WriteString(fromEncloseFragment)
	if view.Alias != "" {
		sb.WriteString(asFragment)
		sb.WriteString(view.Alias)
	}

	hasCriteria := false
	whereFragmentAdded := false

	if batchData.ColumnName != "" {
		sb.WriteString(whereFragment)
		whereFragmentAdded = true
		sb.WriteString(batchData.ColumnName)
		sb.WriteString(inFragment)
		for i := range batchData.Placeholders {
			if i != 0 {
				sb.WriteString(separatorFragment)
			}
			sb.WriteString(placeholderFragment)
		}
		sb.WriteString(")")
	}

	if view.Criteria != nil {
		if !whereFragmentAdded {
			sb.WriteString(whereFragment)
			whereFragmentAdded = true
		}
		sb.WriteString(view.Criteria.Expression)
		hasCriteria = true
	}

	if view.CanUseClientCriteria() && selector != nil && selector.Criteria != nil {
		if hasCriteria {
			sb.WriteString(andFragment)
			sb.WriteString(selector.Criteria.Expression)
			sb.WriteString(encloseFragment)
		} else {
			if !whereFragmentAdded {
				sb.WriteString(whereFragment)
				whereFragmentAdded = true
			}
			sb.WriteString(selector.Criteria.Expression)
		}
	}

	orderBy := view.Selector.OrderBy
	limit := view.LimitWithSelector(selector)
	offset := 0

	if selector != nil {
		if view.CanUseClientOrderBy() && selector.OrderBy != "" {
			orderBy = selector.OrderBy
		}

		if view.CanUseClientOffset() && selector.Offset > 0 {
			offset = selector.Offset
		}
	}

	offset += batchData.CurrentlyRead
	if limit == 0 {
		limit = batchData.BatchReadSize
	} else if limit != 0 {
		toRead := limit - batchData.BatchReadSize - batchData.CurrentlyRead
		if toRead >= 0 {
			limit = batchData.BatchReadSize
		} else if toRead < 0 {
			limit = batchData.BatchReadSize + toRead
		}
	}

	if orderBy != "" {
		if _, ok := view.ColumnByName(orderBy); !ok {
			return "", fmt.Errorf("invalid orderBy column: %v", orderBy)
		}
		sb.WriteString(orderByFragment)
		sb.WriteString(orderBy)
	}

	if limit > 0 {
		sb.WriteString(limitFragment)
		sb.WriteString(strconv.Itoa(limit))
	}

	if offset > 0 {
		sb.WriteString(offsetFragment)
		sb.WriteString(strconv.Itoa(offset))
	}

	return sb.String(), nil
}
