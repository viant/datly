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
	KindDataView    Kind = "data_view"
	KindHeader      Kind = "header"
	KindQuery       Kind = "query"
	KindPath        Kind = "path"
	KindCookie      Kind = "cookie"
	KindRequestBody Kind = "body"
	KindEnvironment Kind = "env"
	KindLiteral     Kind = "literal"
	KindStructQL    Kind = "structql"
	KindParam       Kind = "param"
)

//Validate checks if Kind is valid.
func (k Kind) Validate() error {
	switch k {
	case DataViewKind, KindPath, KindQuery, KindHeader, KindCookie, KindRequestBody, KindEnvironment, KindLiteral, KindStructQL, KindParam:
		return nil
	}

	return fmt.Errorf("unsupported location Kind %v", k)
}

//ParamName represents name of parameter in given Location.Kind
//i.e. if you want to extract lang from query string: ?foo=bar&lang=eng
//required Kind is KindQuery and ParamName `lang`
type ParamName string

//Validate checks if ParamName is valid
func (p ParamName) Validate(kind Kind) error {
	if p == "" && kind != KindRequestBody {
		return fmt.Errorf("param name can't be empty")
	}

	switch kind {
	case DataViewKind, KindPath, KindQuery, KindHeader, KindCookie, KindRequestBody, KindLiteral, KindStructQL, KindParam:
		return nil
	case KindEnvironment:
		if os.Getenv(string(p)) == "" {
			return fmt.Errorf("env variable %s not set", p)
		}

		return nil
	}

	return fmt.Errorf("unsupported param name %v for location kind %v", p, kind)
}
