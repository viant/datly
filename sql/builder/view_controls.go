package builder

import (
	"strings"

	"github.com/viant/datly/spec"
	xstate "github.com/viant/xdatly/state"
)

func MergeViewControlsWithSelector(controls *spec.ViewControls, selector *xstate.Selector) *spec.ViewControls {
	if controls == nil && selector == nil {
		return nil
	}
	merged := controls.Clone()
	if merged == nil {
		merged = &spec.ViewControls{}
	}
	if selector != nil {
		if selector.OrderBy != "" {
			merged.OrderBy = selector.OrderBy
		}
		effectiveLimit := 0
		if selector.Limit > 0 {
			limit := selector.Limit
			merged.Limit = &limit
			effectiveLimit = limit
		} else if merged.Limit != nil {
			effectiveLimit = *merged.Limit
		}
		if selector.Offset > 0 {
			offset := selector.Offset
			merged.Offset = &offset
		} else if selector.Page > 0 && effectiveLimit > 0 {
			offset := effectiveLimit * (selector.Page - 1)
			merged.Offset = &offset
		}
	}
	if merged.IsZero() {
		return nil
	}
	return merged
}

// NonWindowControls returns the merged controls for the root SQL with the
// pagination window stripped so derived SQL can safely wrap the full parent
// query through $View.NonWindowSQL.
func NonWindowControls(controls *spec.ViewControls, selector *xstate.Selector) *spec.ViewControls {
	merged := MergeViewControlsWithSelector(controls, selector)
	if merged == nil {
		return nil
	}
	merged.Limit = nil
	merged.Offset = nil
	if merged.IsZero() {
		return nil
	}
	return merged
}

func NonWindowSelector(selector *xstate.Selector) *xstate.Selector {
	if selector == nil {
		return nil
	}
	cloned := *selector
	cloned.Limit = 0
	cloned.Offset = 0
	cloned.Page = 0
	return &cloned
}

func shouldFilterDefaultGroupedOrder(options *builderOptions) bool {
	if options == nil || len(options.projection) == 0 || options.view == nil || !options.view.IsGroupable() {
		return false
	}
	return options.selector == nil || strings.TrimSpace(options.selector.OrderBy) == ""
}
