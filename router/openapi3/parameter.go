package openapi3

import (
	"context"
	"encoding/json"
)

type (
	Parameters    []*Parameter
	ParametersMap map[string]*Parameter

	Parameter struct {
		Extension       `json:",omitempty" yaml:",inline"`
		Ref             string      `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Name            string      `json:"name,omitempty" yaml:"name,omitempty"`
		In              string      `json:"in,omitempty" yaml:"in,omitempty"`
		Description     string      `json:"description,omitempty" yaml:"description,omitempty"`
		Style           string      `json:"style,omitempty" yaml:"style,omitempty"`
		Explode         *bool       `json:"explode,omitempty" yaml:"explode,omitempty"`
		AllowEmptyValue bool        `json:"allowEmptyValue,omitempty" yaml:"allowEmptyValue,omitempty"`
		AllowReserved   bool        `json:"allowReserved,omitempty" yaml:"allowReserved,omitempty"`
		Deprecated      bool        `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`
		Required        bool        `json:"required,omitempty" yaml:"required,omitempty"`
		Schema          *Schema     `json:"schema,omitempty" yaml:"schema,omitempty"`
		Example         interface{} `json:"example,omitempty" yaml:"example,omitempty"`
		Examples        Examples    `json:"examples,omitempty" yaml:"examples,omitempty"`
		Content         Content     `json:"content,omitempty" yaml:"content,omitempty"`
	}
)

func (p *Parameter) UnmarshalJSON(b []byte) error {
	type temp Parameter
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*p = Parameter(tmp)
	return p.Extension.UnmarshalJSON(b)
}

func (p *Parameter) MarshalJSON() ([]byte, error) {
	type temp Parameter
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

func (p *Parameter) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Parameter
	tmp := temp(*p)
	err := fn(&tmp)
	if err != nil {
		return err
	}
	if tmp.Ref == "" {
		*p = Parameter(tmp)
		return nil
	}
	session := LookupSession(ctx)
	param, err := session.LookupParameter(session.Location, tmp.Ref)
	if err != nil {
		return err
	}
	*p = *param
	return err
}
