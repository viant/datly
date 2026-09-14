package spec

import "strings"

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
