package openapi3

import (
	"context"
	"encoding/json"
)

// Paths represents a path defined by OpenAPI/Swagger standard version 3.0.
type (
	Paths    map[string]*PathItem
	PathItem struct {
		Extension   `json:",omitempty" yaml:",inline"`
		Ref         string     `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Summary     string     `json:"summary,omitempty" yaml:"summary,omitempty"`
		Description string     `json:"description,omitempty" yaml:"description,omitempty"`
		Connect     *Operation `json:"connect,omitempty" yaml:"connect,omitempty"`
		Delete      *Operation `json:"delete,omitempty" yaml:"delete,omitempty"`
		Get         *Operation `json:"get,omitempty" yaml:"get,omitempty"`
		Head        *Operation `json:"head,omitempty" yaml:"head,omitempty"`
		Options     *Operation `json:"options,omitempty" yaml:"options,omitempty"`
		Patch       *Operation `json:"patch,omitempty" yaml:"patch,omitempty"`
		Post        *Operation `json:"post,omitempty" yaml:"post,omitempty"`
		Put         *Operation `json:"put,omitempty" yaml:"put,omitempty"`
		Trace       *Operation `json:"trace,omitempty" yaml:"trace,omitempty"`
		Servers     Servers    `json:"servers,omitempty" yaml:"servers,omitempty"`
		Parameters  Parameters `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	}
)

func (p *PathItem) UnmarshalJSON(b []byte) error {
	type temp PathItem
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*p = PathItem(tmp)
	return p.Extension.UnmarshalJSON(b)
}

func (p *PathItem) MarshalJSON() ([]byte, error) {
	type temp PathItem
	tmp := temp(*p)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(p.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(p.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}



func (s *PathItem) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp PathItem
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
	*s = PathItem(tmp)
	return nil
}