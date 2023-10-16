package openapi3

import (
	"context"
	"encoding/json"
)

// Encoding is specified by OpenAPI/Swagger 3.0 standard.
type Encoding struct {
	Extension     `json:",omitempty" yaml:",inline"`
	ContentType   string  `json:"contentType,omitempty" yaml:"contentType,omitempty"`
	Headers       Headers `json:"headers,omitempty" yaml:"headers,omitempty"`
	Style         string  `json:"style,omitempty" yaml:"style,omitempty"`
	Explode       *bool   `json:"explode,omitempty" yaml:"explode,omitempty"`
	AllowReserved bool    `json:"allowReserved,omitempty" yaml:"allowReserved,omitempty"`
}

func (e *Encoding) UnmarshalJSON(b []byte) error {
	type temp Encoding
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*e = Encoding(tmp)
	return e.Extension.UnmarshalJSON(b)
}

func (e *Encoding) MarshalJSON() ([]byte, error) {
	type temp Encoding
	tmp := temp(*e)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(e.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(e.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (e *Encoding) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Encoding
	tmp := temp(*e)
	err := fn(&tmp)
	if err != nil {
		return err
	}
	ext := CustomExtension{}
	err = fn(&ext)
	if err != nil {
		return err
	}
	tmp.Extension = Extension(ext)
	*e = Encoding(tmp)
	return nil
}
