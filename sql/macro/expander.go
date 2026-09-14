package macro

import (
	"fmt"

	"github.com/viant/sqlx/metadata/info"
)

// ParentKeyExpander owns one invocation's parent-key SQL rendering inputs.
// Both plain builder macros and compiled Velty $View methods delegate here.
type ParentKeyExpander struct {
	Dialect       *info.Dialect
	ScalarValues  []any
	CompositeRows [][]interface{}
	Exclude       bool
}

func (e ParentKeyExpander) Expand(call ParentKeyCall) (string, []any, error) {
	if !parentKeyMethods[call.Method] {
		return "", nil, fmt.Errorf("unsupported $View parent-key method %q", call.Method)
	}
	if err := validateParentKeyPrefix(call.Prefix); err != nil {
		return "", nil, err
	}
	for _, column := range call.Columns {
		if err := validateColumn(column); err != nil {
			return "", nil, fmt.Errorf("invalid $View.%s column: %w", call.Method, err)
		}
	}
	if e.Exclude {
		return "", nil, nil
	}
	fragment, args := parentKeyFragment(call, e.Dialect, e.ScalarValues, e.CompositeRows)
	return fragment, args, nil
}
