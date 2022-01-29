package reader

import (
	"github.com/viant/datly/v1/data"
	"strings"
)

const (
	selectFragment    = "SELECT "
	separatorFragment = ", "
	fromFragment      = " FROM "
)

//Builder represent SQL Builder
// TODO: use cases for selector attributes from client side
type Builder struct {
	view data.View
}

//NewBuilder creates Builder instance
func NewBuilder() *Builder {
	return &Builder{}
}

//Build builds SQL Select statement
func (b *Builder) Build(columns []string, tableName string) string {
	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	for i := 0; i < len(columns); i++ {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		sb.WriteString(columns[i])
	}
	sb.WriteString(fromFragment)
	sb.WriteString(tableName)
	return sb.String()
}
