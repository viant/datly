package locator

import (
	"context"
	"github.com/google/uuid"
	"github.com/viant/datly/view/state/kind"
	"strings"
	"time"
)

type Generator struct{}

func (v *Generator) Names() []string {
	return nil
}

func (v *Generator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	switch strings.ToLower(name) {
	case "nil":
		return nil, true, nil
	case "true":
		return true, true, nil
	case "false":
		return false, true, nil
	case "zero":
		return 0, true, nil
	case "one":
		return 1, true, nil
	case "now", "current_time":
		return time.Now(), true, nil
	case "uuid":
		UUID := uuid.New()
		return UUID.String(), true, nil
	case "empty":
		return "", true, nil
	}
	return nil, false, nil
}

// NewGenerator returns Generator locator
func NewGenerator(_ ...Option) (kind.Locator, error) {
	ret := &Generator{}
	return ret, nil
}
