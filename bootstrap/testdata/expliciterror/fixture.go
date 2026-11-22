package expliciterror

import (
	"reflect"

	"github.com/viant/datly/spec"
	"github.com/viant/xdatly"
)

type Input struct{}
type Output struct{}
type ErrorBody struct {
	Message string `json:"message"`
}

type Component struct {
	Contract xdatly.Component[Input, Output] `component:"typedError,path=/typed-error,method=GET"`
}

func (Component) DatlyErrorOutputs() []spec.ErrorOutput {
	return []spec.ErrorOutput{{Type: reflect.TypeFor[ErrorBody](), Presentation: spec.ErrorOutputLegacyText}}
}

var Linked = new(Component)
