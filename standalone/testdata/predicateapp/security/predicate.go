package security

import (
	"context"
	"github.com/viant/xdatly/predicate"
)

type Minimum struct{}

func (*Minimum) Compute(_ context.Context, value any) (*predicate.Criteria, error) {
	return &predicate.Criteria{Expression: "id >= ?", Placeholders: []any{value.(int)}}, nil
}
