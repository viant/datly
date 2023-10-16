package openapi3

import (
	"context"
	"encoding/json"
)

type (
	Tags []*Tag

	Tag struct {
		Extension    `json:",omitempty" yaml:",inline"`
		Name         string                 `json:"name,omitempty" yaml:"name,omitempty"`
		Description  string                 `json:"description,omitempty" yaml:"description,omitempty"`
		ExternalDocs *ExternalDocumentation `json:"externalDocs,omitempty" yaml:"externalDocs,omitempty"`
	}
)

func (t *Tag) UnmarshalJSON(b []byte) error {
	type temp Tag
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*t = Tag(tmp)
	return t.Extension.UnmarshalJSON(b)
}

func (t *Tag) MarshalJSON() ([]byte, error) {
	type temp Tag
	tmp := temp(*t)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(t.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(t.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (s *Tag) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Tag
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
	*s = Tag(tmp)
	return nil
}
