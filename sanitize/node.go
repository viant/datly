package sanitize

import (
	"github.com/viant/datly/data"
	"strings"
)

type Node interface {
	Sanitize(sb *strings.Builder, columns data.Columns) error
}
