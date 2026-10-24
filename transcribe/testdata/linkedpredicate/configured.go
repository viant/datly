package linkedpredicate

import (
	"context"
	"fmt"
	"github.com/viant/xdatly/predicate"
)

// ConfiguredThreshold demonstrates typed authored arguments without changing
// the legacy Threshold handler contract.
type ConfiguredThreshold struct{}

var LinkedConfiguredHandler any = &ConfiguredThreshold{}

func init() { _ = LinkedConfiguredHandler }
func (*ConfiguredThreshold) Compute(ctx context.Context, value any, args ...any) (*predicate.Criteria, error) {
	options, err := predicate.Pairs(args...)
	if err != nil {
		return nil, err
	}
	offset := 0
	for name, value := range options {
		switch name {
		case "offset":
			var ok bool
			offset, ok = value.(int)
			if !ok {
				return nil, fmt.Errorf("offset requires int")
			}
		case "enabled":
			enabled, ok := value.(bool)
			if !ok || !enabled {
				return nil, fmt.Errorf("enabled requires true")
			}
		default:
			return nil, fmt.Errorf("unknown option %q", name)
		}
	}
	result, err := (&Threshold{}).Compute(ctx, value)
	if err != nil {
		return nil, err
	}
	result.Placeholders[0] = result.Placeholders[0].(int) + offset
	return result, nil
}
