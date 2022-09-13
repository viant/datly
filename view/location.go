package view

import (
	"fmt"
	"os"
)

//Kind represents parameter location
//Parameter value can be retrieved from the i.e. HTTP Header, Path Variable or using other View
type Kind string

const (
	DataViewKind    Kind = "data_view"
	HeaderKind      Kind = "header"
	QueryKind       Kind = "query"
	PathKind        Kind = "path"
	CookieKind      Kind = "cookie"
	RequestBodyKind Kind = "body"
	EnvironmentKind Kind = "env"
	LiteralKind     Kind = "literal"
)

//Validate checks if Kind is valid.
func (k Kind) Validate() error {
	switch k {
	case DataViewKind, PathKind, QueryKind, HeaderKind, CookieKind, RequestBodyKind, EnvironmentKind, LiteralKind:
		return nil
	}

	return fmt.Errorf("unsupported location Kind %v", k)
}

//ParamName represents name of parameter in given Location.Kind
//i.e. if you want to extract lang from query string: ?foo=bar&lang=eng
//required Kind is QueryKind and ParamName `lang`
type ParamName string

//Validate checks if ParamName is valid
func (p ParamName) Validate(kind Kind) error {
	if p == "" && kind != RequestBodyKind {
		return fmt.Errorf("param name can't be empty")
	}

	switch kind {
	case DataViewKind, PathKind, QueryKind, HeaderKind, CookieKind, RequestBodyKind, LiteralKind:
		return nil
	case EnvironmentKind:
		if os.Getenv(string(p)) == "" {
			return fmt.Errorf("env variable %s not set", p)
		}

		return nil
	}

	return fmt.Errorf("unsupported param name %v for location kind %v", p, kind)
}
