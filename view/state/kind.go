package state

import (
	"fmt"
)

// Kind represents parameter location
// Parameter value can be retrieved from the i.e. HTTP Header, path Variable or using other View
type Kind string

const (
	KindDataView    Kind = "data_view"
	KindHeader      Kind = "header"
	KindQuery       Kind = "query"
	KindPath        Kind = "path"
	KindCookie      Kind = "cookie"
	KindRequestBody Kind = "body"
	KindEnvironment Kind = "env"
	KindConst       Kind = "const"
	KindLiteral     Kind = "literal"

	KindParam   Kind = "param"
	KindRequest Kind = "http_request"
	KindObject  Kind = "object"

	KindRepeated Kind = "repeated"

	KindOutput    Kind = "output"    //reader output
	KindState     Kind = "state"     //input state
	KindComponent Kind = "component" //input state
	KindContext   Kind = "context"   //global context based state

)

// Validate checks if Kind is valid.
func (k Kind) Validate() error {
	switch k {
	case KindDataView, KindPath, KindQuery, KindHeader, KindCookie, KindRequestBody, KindEnvironment, KindConst, KindLiteral, KindParam, KindRequest, KindRepeated, KindObject, KindOutput, KindState, KindContext, KindComponent:
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
	case KindConst:
		return 7
	case KindParam:
		return 8
	case KindRequest:
		return 9
	case KindOutput:
		return 10
	case KindState:
		return 11
	case KindContext:
		return 12
	case KindComponent:
		return 13
	}
	return -1
}
