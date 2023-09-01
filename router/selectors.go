package router

import (
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/view"
	vstate "github.com/viant/datly/view/state"
)

type (
	RequestMetadata struct {
		URI      string
		MainView *view.View
	}
)

func (p *RequestParams) convert(isSpecified bool, raw string, param *vstate.Parameter) (interface{}, error) {
	if raw == "" && param.IsRequired() {
		return nil, requiredParamErr(param)
	}

	if !isSpecified {
		return nil, nil
	}

	dateFormat := ""
	convert, _, err := converter.Convert(raw, param.Schema.Type(), true, dateFormat)
	return convert, err
}

func requiredParamErr(param *vstate.Parameter) error {
	return fmt.Errorf("parameter %v is required", param.Name)
}
