package router

import (
	"fmt"
	"github.com/viant/datly/oas/coding"
	"github.com/viant/datly/oas/openapi3"
	soperation "github.com/viant/datly/oas/operation"
	"github.com/viant/datly/oas/spec"
)

type (
	operation struct {
		op               openapi3.Operation
		queryEncoder     coding.Encoder
		requestEncoder   map[string]*content
		responseDecoder  map[string]*content
		responseProvider spec.Provider
		service          soperation.Service
	}


)

func (o *operation) Init() error {
	o.requestEncoder = make(map[string]*content)
	o.responseDecoder = make(map[string]*content)
	if o.op.RequestBody != nil {
		content := o.op.RequestBody.Content
		if len(content) > 0 {
			for contentType, v := range content {
				mType, err := initContent(contentType, v)
				if err != nil {
					return err
				}
				o.requestEncoder[contentType] = mType
			}
		}
	}

	return nil
}

func (o *operation) Validate() error {
	return nil
}
