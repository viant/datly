package reader

import (
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
func (b *Builder) Build(view *data.View, selectorInUse data.Selector) string {
	sb := strings.Builder{}
	sb.WriteString(selectFragment)

	var col *data.Column
	var i int
	for i, col = range selectorInUse.GetColumns() {
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

	if selectorInUse.GetOrderBy() != "" {
		sb.WriteString(orderByFragment)
		sb.WriteString(selectorInUse.GetOrderBy())
	}

	if selectorInUse.GetLimit() > 0 {
		sb.WriteString(limitFragment)
		sb.WriteString(strconv.Itoa(selectorInUse.GetLimit()))
	}

	if selectorInUse.GetOffset() > 0 {
		sb.WriteString(offsetFragment)
		sb.WriteString(strconv.Itoa(selectorInUse.GetOffset()))
	}
	return sb.String()
}
