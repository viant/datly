package marshaller

import (
	"bytes"
	"encoding/json"
	"github.com/francoispqt/gojay"
)

type JSON struct{}

func (j *JSON) Unmarshal(bytes []byte, dest interface{}) error {
	return json.Unmarshal(bytes, dest)
}

func (j *JSON) Marshal(src interface{}) ([]byte, error) {
	return json.Marshal(src)
}

type Gojay struct{}

func (g *Gojay) Unmarshal(bytes []byte, dest interface{}) error {
	return gojay.Unmarshal(bytes, dest)
}

func (g *Gojay) Marshal(src interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gojay.NewEncoder(&buf).Encode(src); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
