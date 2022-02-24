package sanitize

import (
	"fmt"
	"github.com/viant/datly/data"
	"strings"
)

type Selector struct {
	Name string
}

func (s *Selector) Adjust(sb *strings.Builder, columns data.Columns) error {
	col, err := columns.Lookup(s.Name)
	if err != nil {
		return err
	}

	if !col.Filterable {
		return fmt.Errorf("column %v is not filterable", s.Name)
	}

	sb.WriteString(col.Name)
	return nil
}
