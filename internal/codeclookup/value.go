package codeclookup

import (
	"context"
	"fmt"
)

// Value preserves the distinction between an omitted declared optional input
// and an unknown or required dependency. Codecs own their optional fallbacks;
// lookup must not invent input values or suppress resolver failures.
func Value(ctx context.Context, name string, optional map[string]bool, resolve func(context.Context, string) (any, bool, error)) (any, error) {
	if resolve == nil {
		if optional[name] {
			return nil, nil
		}
		return nil, fmt.Errorf("codec input parameter %s is unavailable", name)
	}
	value, found, err := resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	if !found {
		if optional[name] {
			return nil, nil
		}
		return nil, fmt.Errorf("codec input parameter %s is unavailable", name)
	}
	return value, nil
}
