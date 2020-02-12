package contract

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/generic"
	ogojay "github.com/viant/datly/generic/gojay"
)

//Data represents data
type Data map[string]interface{}

func (r Data) MarshalJSON() ([]byte, error) {
	builder := new(bytes.Buffer)
	enc := gojay.NewEncoder(builder)
	provider := generic.NewProvider()
	provider.SetOmitEmpty(true)
	genericResponse, err := provider.Object(r)
	if err != nil {
		return nil, err
	}
	marshaler := &ogojay.Object{genericResponse}
	err = enc.Encode(marshaler)
	if err != nil {
		return nil, err
	}
	return builder.Bytes(), err
}
