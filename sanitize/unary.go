package sanitize

import (
	"github.com/viant/datly/data"
	"strings"
)

type Unary struct {
	X        Node
	Operator string
}

func (u *Unary) Adjust(sb *strings.Builder, columns data.Columns) error {
	sb.WriteString(u.Operator)
	err := u.X.Adjust(sb, columns)
	if err != nil {
		return err
	}
	return nil
}
