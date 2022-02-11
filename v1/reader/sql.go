package reader

import (
	"fmt"
	"github.com/viant/datly/v1/data"
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
)

//Builder represent SQL Builder
type Builder struct {
}

//NewBuilder creates Builder instance
func NewBuilder() *Builder {
	return &Builder{}
}

//Build builds SQL Select statement
// TODO: add client selector
func (b *Builder) Build(view *data.View, selector *data.Selector) (string, error) {
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

	orderBy := view.Selector.OrderBy
	limit := view.Selector.Limit
	offset := 0

	if selector != nil {
		if selector.OrderBy != "" {
			orderBy = selector.OrderBy
		}
		if selector.Limit != 0 {
			limit = selector.Limit
		}

		if selector.Offset > 0 {
			offset = selector.Offset
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
		sb.WriteString(strconv.Itoa(selector.Offset))
	}

	return sb.String(), nil
}
