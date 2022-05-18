package sanitize

import (
	"github.com/viant/datly/view"
	"strings"
)

type Unary struct {
	X        Node
	Operator string
}

func (u *Unary) Sanitize(sb *strings.Builder, columns view.Columns) error {
	sb.WriteString(u.Operator)
	err := u.X.Sanitize(sb, columns)
	if err != nil {
		return err
	}
	return nil
}
