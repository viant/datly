package spec

import (
	"fmt"
	"slices"
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

// SetPermission authors a permission independently of input binding. In
// particular, an explicit false must not be replaced by binding inference.
func (s *Selector) SetPermission(property SelectorProperty, enabled bool) error {
	field, err := s.permissionField(property)
	if err != nil {
		return err
	}
	*field = enabled
	if !s.PermissionSpecified(property) {
		s.Specified = append(s.Specified, property)
		slices.Sort(s.Specified)
	}
	return nil
}

func (s *Selector) PermissionSpecified(property SelectorProperty) bool {
	return s != nil && slices.Contains(s.Specified, property)
}

func (s *Selector) permissionField(property SelectorProperty) (*bool, error) {
	if s == nil {
		return nil, fmt.Errorf("selector policy is required")
	}
	switch property {
	case SelectorPropertyFields:
		return &s.AllowFields, nil
	case SelectorPropertyOrderBy:
		return &s.AllowOrderBy, nil
	case SelectorPropertyCriteria:
		return &s.AllowCriteria, nil
	case SelectorPropertyLimit:
		return &s.AllowLimit, nil
	case SelectorPropertyOffset:
		return &s.AllowOffset, nil
	case SelectorPropertyPage:
		return &s.AllowPage, nil
	default:
		return nil, fmt.Errorf("unsupported query selector property %q", property)
	}
}

// EnableQuerySelector supplies a default permission for a declared binding.
// An explicitly authored view policy always wins, including an explicit deny.
func (v *View) EnableQuerySelector(property SelectorProperty) error {
	if v == nil {
		return fmt.Errorf("query selector view is required")
	}
	if v.Selector == nil {
		v.Selector = &Selector{}
	}
	field, err := v.Selector.permissionField(property)
	if err != nil {
		return err
	}
	if !v.Selector.PermissionSpecified(property) {
		*field = true
	}
	if property == SelectorPropertyCriteria && *field && len(v.Selector.Filterable) == 0 {
		// Keep declared criteria bounded to the compiled projection unless
		// the author supplies a narrower allowlist.
		v.Selector.Filterable = []FieldPath{"*"}
	}
	return nil
}
