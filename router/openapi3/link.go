package openapi3

import (
	"context"
	"encoding/json"
)

type (
	Links map[string]*Link
	Link struct {
		Extension    `json:",omitempty" yaml:",inline"`
		Ref          string                 `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		OperationID  string                 `json:"operationId,omitempty" yaml:"operationId,omitempty"`
		OperationRef string                 `json:"operationRef,omitempty" yaml:"operationRef,omitempty"`
		Description  string                 `json:"description,omitempty" yaml:"description,omitempty"`
		Parameters   map[string]interface{} `json:"parameters,omitempty" yaml:"parameters,omitempty"`
		Server       *Server                `json:"server,omitempty" yaml:"server,omitempty"`
		RequestBody  interface{}            `json:"requestBody,omitempty" yaml:"requestBody,omitempty"`
	}
)

func (l *Link) UnmarshalJSON(b []byte) error {
	type temp Link
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*l = Link(tmp)
	return l.Extension.UnmarshalJSON(b)
}

func (l *Link) MarshalJSON() ([]byte, error) {
	type temp Link
	tmp := temp(*l)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(l.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(l.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (s *Link) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Link
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
	*s = Link(tmp)
	if tmp.Ref == "" {
		return nil
	}
	session := LookupSession(ctx)
	param, err := session.LookupLink(session.Location, tmp.Ref)
	if err == nil {
		*s = *param
	}
	return err
}
