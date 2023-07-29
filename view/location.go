package view

import (
	"fmt"
	"os"
	"strings"
)

// Kind represents parameter location
// Parameter value can be retrieved from the i.e. HTTP Header, Path Variable or using other View
type Kind string

const (
	KindDataView    Kind = "data_view"
	KindHeader      Kind = "header"
	KindQuery       Kind = "query"
	KindPath        Kind = "path"
	KindCookie      Kind = "cookie"
	KindRequestBody Kind = "body"
	KindEnvironment Kind = "env"
	KindLiteral     Kind = "literal"
	KindParam       Kind = "param"

	KindPredicate Kind = "predicate"
	KindRequest   Kind = "http_request"
	KindState     Kind = "state"
	KindGroup     Kind = "group"
)

// Validate checks if Kind is valid.
func (k Kind) Validate() error {
	switch k {
	case KindDataView, KindPath, KindQuery, KindHeader, KindCookie, KindRequestBody, KindEnvironment, KindLiteral, KindParam, KindRequest, KindState, KindGroup:
		return nil
	}

	return fmt.Errorf("unsupported location Kind %v", k)
}

func (k Kind) Ordinal() int {
	switch k {
	case KindDataView:
		return 0
	case KindHeader:
		return 1
	case KindQuery:
		return 2
	case KindPath:
		return 3
	case KindCookie:
		return 4
	case KindRequestBody:
		return 5
	case KindEnvironment:
		return 6
	case KindLiteral:
		return 7
	case KindParam:
		return 8
	case KindRequest:
		return 9
	case KindState:
		return 10
	}
	return -1
}

// ParamName represents name of parameter in given Location.Kind
// i.e. if you want to extract lang from query string: ?foo=bar&lang=eng
// required Kind is KindQuery and ParamName `lang`
type ParamName string

// Validate checks if ParamName is valid
func (p ParamName) Validate(kind Kind) error {
	switch kind {
	case KindGroup:
		split := strings.Split(string(p), ",")
		if len(split) == 0 {
			return fmt.Errorf("param name can't be empty")
		}

		if len(split) == 1 {
			return fmt.Errorf("param Group must contain at least 2 params")
		}

		return nil
	case KindRequest, KindLiteral, KindRequestBody, KindState:
		return nil
	case KindDataView, KindPath, KindQuery, KindHeader, KindCookie, KindParam:
		if p == "" {
			return fmt.Errorf("param name can't be empty")
		}

		return nil
	case KindEnvironment:
		if os.Getenv(string(p)) == "" {
			return fmt.Errorf("env variable %s not set", p)
		}
		return nil
	}
	return fmt.Errorf("unsupported param name %v for location kind %v", p, kind)
}
