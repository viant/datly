package linkedpredicate

import (
	"context"
	"fmt"

	"github.com/viant/xdatly/predicate"
)

// Threshold is compiled into the transcribing binary before DQL names it.
type Threshold struct{}

// Retain this real handler identity in the linked package without a separate
// marker type or runtime registration call.
var LinkedHandler any = &Threshold{}

func init() { _ = LinkedHandler }

func (*Threshold) Compute(_ context.Context, value any) (*predicate.Criteria, error) {
	minimum, ok := value.(int)
	if !ok {
		return nil, fmt.Errorf("threshold requires int input, got %T", value)
	}
	return &predicate.Criteria{Expression: "id >= ?", Placeholders: []any{minimum}}, nil
}
