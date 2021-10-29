package openapi3

import (
	"context"
	"encoding/json"
)

type ExternalDocumentation struct {
	Extension   `json:",omitempty" yaml:",inline"`
	Description string `json:"description,omitempty"`
	URL         string `json:"url,omitempty"`
}

func (e *ExternalDocumentation) UnmarshalJSON(b []byte) error {
	type temp ExternalDocumentation
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*e = ExternalDocumentation(tmp)
	return e.Extension.UnmarshalJSON(b)
}

func (e *ExternalDocumentation) MarshalJSON() ([]byte, error) {
	type temp ExternalDocumentation
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



func (e *ExternalDocumentation) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp ExternalDocumentation
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
	*e = ExternalDocumentation(tmp)
	return nil
}