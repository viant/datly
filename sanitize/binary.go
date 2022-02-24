package sanitize

import (
	"github.com/viant/datly/data"
	"strings"
)

type Binary struct {
	X        Node //left operand
	Operator string
	Y        Node // right operand

}

func (b *Binary) Adjust(sb *strings.Builder, columns data.Columns) error {
	err := b.X.Adjust(sb, columns)
	if err != nil {
		return err
	}

	sb.WriteString(" ")
	sb.WriteString(b.Operator)
	sb.WriteString(" ")

	err = b.Y.Adjust(sb, columns)
	if err != nil {
		return err
	}

	return nil
}
