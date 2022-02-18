package sql

import (
	"fmt"
	"strings"
)

type Literal struct {
	Value string
	Kind  int
}

func (l *Literal) Validate(allowed map[string]int) error {
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
	return nil
}
