package openapi3

import (
	"context"
	"encoding/json"
)

// Components is specified by OpenAPI/Swagger standard version 3.0.
type Components struct {
	Extension       `json:",omitempty" yaml:",inline"`
	Schemas         Schemas         `json:"schemas,omitempty" yaml:"schemas,omitempty"`
	Parameters      ParametersMap   `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Headers         Headers         `json:"headers,omitempty" yaml:"headers,omitempty"`
	RequestBodies   RequestBodies   `json:"requestBodies,omitempty" yaml:"requestBodies,omitempty"`
	Responses       Responses       `json:"responses,omitempty" yaml:"responses,omitempty"`
	SecuritySchemes SecuritySchemes `json:"securitySchemes,omitempty" yaml:"securitySchemes,omitempty"`
	Examples        Examples        `json:"examples,omitempty" yaml:"examples,omitempty"`
	Links           Links           `json:"links,omitempty" yaml:"links,omitempty"`
	Callbacks       Callbacks       `json:"callbacks,omitempty" yaml:"callbacks,omitempty"`
}

func (c *Components) UnmarshalJSON(b []byte) error {
	type temp Components
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*c = Components(tmp)
	return c.Extension.UnmarshalJSON(b)
}

func (c *Components) MarshalJSON() ([]byte, error) {
	type temp Components
	tmp := temp(*c)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(c.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(c.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}


func (s *Components) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Components
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
	*s = Components(tmp)
	session := LookupSession(ctx)
	session.RegisterComponents(session.Location, s)
	return nil
}