package spec

import (
	"fmt"
	"strings"
)

type SelectorProperty string

const (
	SelectorPropertyFields   SelectorProperty = "fields"
	SelectorPropertyOrderBy  SelectorProperty = "orderBy"
	SelectorPropertyOffset   SelectorProperty = "offset"
	SelectorPropertyLimit    SelectorProperty = "limit"
	SelectorPropertyPage     SelectorProperty = "page"
	SelectorPropertyCriteria SelectorProperty = "criteria"
)

type QuerySelectorBinding struct {
	View     string           `json:"view"`
	Property SelectorProperty `json:"property"`
}

func SelectorPropertyForParam(name string) (SelectorProperty, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "fields":
		return SelectorPropertyFields, true
	case "orderby":
		return SelectorPropertyOrderBy, true
	case "offset":
		return SelectorPropertyOffset, true
	case "limit":
		return SelectorPropertyLimit, true
	case "page":
		return SelectorPropertyPage, true
	case "criteria":
		return SelectorPropertyCriteria, true
	default:
		return "", false
	}
}

// EnableQuerySelector permits the request field that was explicitly declared
// with QuerySelector(view). The declaration itself is the author's grant for
// that one property; unrelated selector inputs remain closed.
func (v *View) EnableQuerySelector(property SelectorProperty) error {
	if v == nil {
		return fmt.Errorf("query selector view is required")
	}
	if v.Selector == nil {
		v.Selector = &Selector{}
	}
	switch property {
	case SelectorPropertyFields:
		v.Selector.AllowFields = true
	case SelectorPropertyOrderBy:
		v.Selector.AllowOrderBy = true
	case SelectorPropertyCriteria:
		v.Selector.AllowCriteria = true
		if len(v.Selector.Filterable) == 0 {
			// A declared Criteria field is the author's opt-in. In the
			// absence of a narrower allowlist, the runtime still restricts
			// names to columns proven by this view's compiled projection.
			v.Selector.Filterable = []FieldPath{"*"}
		}
	case SelectorPropertyLimit:
		v.Selector.AllowLimit = true
	case SelectorPropertyOffset:
		v.Selector.AllowOffset = true
	case SelectorPropertyPage:
		v.Selector.AllowPage = true
	default:
		return fmt.Errorf("unsupported query selector property %q", property)
	}
	return nil
}
