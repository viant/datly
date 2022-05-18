package sanitize

import (
	"github.com/viant/datly/view"
	"strings"
)

type Node interface {
	Sanitize(sb *strings.Builder, columns view.Columns) error
}
