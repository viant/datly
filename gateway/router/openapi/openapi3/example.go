package openapi3

import (
	"context"
	"encoding/json"
)

type (
	Examples map[string]*Example

	Example struct {
		Extension     `json:",omitempty" yaml:",inline"`
		Ref           string      `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Summary       string      `json:"summary,omitempty" yaml:"summary,omitempty"`
		Description   string      `json:"description,omitempty" yaml:"description,omitempty"`
		Value         interface{} `json:"value,omitempty" yaml:"value,omitempty"`
		ExternalValue string      `json:"externalValue,omitempty" yaml:"externalValue,omitempty"`
	}
)

func (e *Example) UnmarshalJSON(b []byte) error {
	type temp Example
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*e = Example(tmp)
	return e.Extension.UnmarshalJSON(b)
}

func (e *Example) MarshalJSON() ([]byte, error) {
	type temp Example
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

func (e *Example) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Example
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
	*e = Example(tmp)
	return nil
}
