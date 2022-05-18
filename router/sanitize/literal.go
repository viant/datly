package sanitize

import (
	"fmt"
	"github.com/viant/datly/router/sanitize/sql"
	"github.com/viant/datly/view"
	"strings"
)

type Literal struct {
	Value string
	Kind  sql.Kind
}

func (l *Literal) Sanitize(sb *strings.Builder, _ view.Columns) error {
	newLines := strings.Count(l.Value, "\n")
	if newLines > 0 {
		return fmt.Errorf("new lines in literal: %v not supported", l.Value)
	}
	comments := strings.Count(l.Value, "--")
	if comments > 0 {
		return fmt.Errorf("coments in literal: %v not supported", l.Value)
	}
	comments = strings.Count(l.Value, "#")
	if comments > 0 {
		return fmt.Errorf("coments in literal: %v not supported", l.Value)
	}
	sb.WriteString(l.Value)
	return nil
}
