package sanitize

import (
	"fmt"
	"github.com/viant/datly/view"
	"strings"
)

type Selector struct {
	Name string
}

func (s *Selector) Sanitize(sb *strings.Builder, columns view.Columns) error {
	col, err := columns.Lookup(s.Name)
	if err != nil {
		return fmt.Errorf("invalid selector: %w", err)
	}

	if !col.Filterable {
		return fmt.Errorf("column %v is not filterable", s.Name)
	}

	sb.WriteString(col.Name)
	return nil
}
