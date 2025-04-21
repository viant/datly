package state

import (
	"fmt"
)

// Kind represents parameter location
// Parameter value can be retrieved from the i.e. HTTP Header, path Variable or using other View
type Kind string

const (
	KindView        Kind = "view"
	KindHeader      Kind = "header"
	KindQuery       Kind = "query"
	KindForm        Kind = "form"
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

	KindOutput Kind = "output" //reader output

	KindAsync Kind = "async" //async jobs/status related information

	KindMeta Kind = "meta" //component/view meta information

	KindState     Kind = "state"     //input state
	KindComponent Kind = "component" //input state
	KindContext   Kind = "context"   //global context based state

	//KindGenerator represents common value generator
	KindGenerator Kind = "generator"

	//KindTransient represents parameter placeholder with no actual value source
	KindTransient Kind = "transient"

	//KindHandler represents handler type
	KindHandler Kind = "handler"
)

// Validate checks if Kind is valid.
func (k Kind) Validate() error {
	switch k {
	case KindView, KindPath, KindForm, KindQuery, KindHeader, KindCookie, KindRequestBody, KindEnvironment, KindConst, KindLiteral, KindParam, KindRequest, KindRepeated, KindObject, KindOutput, KindState, KindContext, KindGenerator, KindTransient, KindHandler, KindComponent, KindMeta, KindAsync:
		return nil
	}

	return fmt.Errorf("unsupported location Kind %v", k)
}

func (k Kind) Ordinal() int {
	switch k {
	case KindView:
		return 0
	case KindHeader:
		return 1
	case KindRequestBody:
		return 2
	case KindQuery:
		return 3
	case KindForm:
		return 3
	case KindPath:
		return 4
	case KindCookie:
		return 5
	case KindEnvironment:
		return 7
	case KindConst:
		return 8
	case KindParam:
		return 9
	case KindRequest:
		return 10
	case KindOutput:
		return 11
	case KindState:
		return 12
	case KindContext:
		return 13
	case KindComponent:
		return 14
	case KindAsync:
		return 15
	case KindMeta:
		return 16
	case KindGenerator:
		return 17
	case KindTransient:
		return 18
	case KindHandler:
		return 19
	}
	return -1
}
