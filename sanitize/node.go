package sanitize

import (
	"github.com/viant/datly/data"
	"strings"
)

type Node interface {
	Adjust(sb *strings.Builder, columns data.Columns) error
}
