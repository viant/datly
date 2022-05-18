package contract

import (
	"github.com/viant/gtly"
	"github.com/viant/gtly/codec/json"
)

//Data represents view
type Data map[string]interface{}

func (r Data) MarshalJSON() ([]byte, error) {
	provider := gtly.NewProvider("Response")
	provider.SetOmitEmpty(true)
	genericResponse, err := provider.Object(r)
	if err != nil {
		return nil, err
	}
	return json.Marshal(genericResponse)
}
