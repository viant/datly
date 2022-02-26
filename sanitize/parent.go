package sanitize

import (
	"github.com/viant/datly/data"
	"strings"
)

type Parentheses struct {
	P Node
}

func (p *Parentheses) Sanitize(sb *strings.Builder, columns data.Columns) error {
	sb.WriteString(" ( ")
	err := p.P.Sanitize(sb, columns)
	if err != nil {
		return err
	}
	sb.WriteString(" ) ")
	return nil
}
