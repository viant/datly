package sanitize

import (
	"github.com/viant/datly/data"
	"strings"
)

type Parentheses struct {
	P Node
}

func (p *Parentheses) Adjust(sb *strings.Builder, columns data.Columns) error {
	sb.WriteString(" ( ")
	err := p.P.Adjust(sb, columns)
	if err != nil {
		return err
	}
	sb.WriteString(" ) ")
	return nil
}
