package data

import (
	"fmt"
)

//Kind represents parameter location, i.e. header, query parameters.
type Kind string

const (
	DataViewKind Kind = "data_view"
	HeaderKind   Kind = "header"
	QueryKind    Kind = "query"
	PathKind     Kind = "path"
	CookieKind   Kind = "cookie"
)

//Validate checks if Kind is valid.
func (k Kind) Validate() error {
	switch k {
	case DataViewKind, PathKind, QueryKind, HeaderKind, CookieKind:
		return nil
	}

	return fmt.Errorf("unsupported location Kind %v", k)
}

//ParamName represents ParamName possible values
type ParamName string

//Validate checks if ParamName is valid
func (p ParamName) Validate(kind Kind) error {
	if p == "" {
		return fmt.Errorf("param name can't be empty")
	}

	switch kind {
	case DataViewKind, PathKind, QueryKind, HeaderKind, CookieKind:
		return nil
	}

	return fmt.Errorf("unsupported param name %v for location kind %v", p, kind)
}
