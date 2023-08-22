package marshal

import (
	"github.com/viant/datly/converter"
	"reflect"
)

type (
	Marshaller struct {
		Type      reflect.Type
		Unmarshal converter.Unmarshaller
	}
)

// NewMarshaller create a marshaller
func NewMarshaller(rType reflect.Type, unmarshal converter.Unmarshaller) *Marshaller {
	return &Marshaller{
		Type:      rType,
		Unmarshal: unmarshal,
	}
}
