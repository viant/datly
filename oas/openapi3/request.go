package openapi3

import (
	"context"
	"encoding/json"
)

type (
	RequestBodies map[string]*RequestBody

	// RequestBody is specified by OpenAPI/Swagger 3.0 standard.
	RequestBody struct {
		Extension   `json:",omitempty" yaml:",inline"`
		Ref         string  `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Description string  `json:"description,omitempty" yaml:"description,omitempty"`
		Required    bool    `json:"required,omitempty" yaml:"required,omitempty"`
		Content     Content `json:"content,omitempty" yaml:"content,omitempty"`
	}
)

func (r *RequestBody) UnmarshalJSON(b []byte) error {
	type temp RequestBody
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*r = RequestBody(tmp)
	return r.Extension.UnmarshalJSON(b)
}

func (r *RequestBody) MarshalJSON() ([]byte, error) {
	type temp RequestBody
	tmp := temp(*r)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(r.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(r.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}


func (s *RequestBody) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp RequestBody
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
	*s = RequestBody(tmp)
	if tmp.Ref == "" {
		return nil
	}
	session := LookupSession(ctx)
	param, err := session.LookupRequestBody(session.Location, tmp.Ref)
	if err == nil {
		*s = *param
	}
	return err
}
