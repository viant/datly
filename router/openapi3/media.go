package openapi3

import (
	"context"
	"encoding/json"
)

// MediaType is specified by OpenAPI/Swagger 3.0 standard.
type MediaType struct {
	Extension `json:",omitempty" yaml:",inline"`
	Schema    *Schema              `json:"schema,omitempty" yaml:"schema,omitempty"`
	Example   interface{}          `json:"example,omitempty" yaml:"example,omitempty"`
	Examples  Examples             `json:"examples,omitempty" yaml:"examples,omitempty"`
	Encoding  map[string]*Encoding `json:"encoding,omitempty" yaml:"encoding,omitempty"`
}

func (m *MediaType) UnmarshalJSON(b []byte) error {
	type temp MediaType
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*m = MediaType(tmp)
	return m.Extension.UnmarshalJSON(b)
}

func (m *MediaType) MarshalJSON() ([]byte, error) {
	type temp MediaType
	tmp := temp(*m)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(m.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(m.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}


func (s *MediaType) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp MediaType
	tmp := temp(*s)
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
	*s = MediaType(tmp)
	return nil
}

