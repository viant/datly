package state

import "fmt"

// Kind represents parameter location
// Parameter value can be retrieved from the i.e. HTTP Header, Path Variable or using other View
type Kind string

func (k Kind) Validate() error {
	switch k {
	case KindDataView, KindPath, KindQuery, KindHeader, KindCookie, KindRequestBody, KindEnvironment, KindLiteral, KindParam, KindRequest:
		return nil
	}
	return fmt.Errorf("unsupported location Kind %v", k)
}

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
)
