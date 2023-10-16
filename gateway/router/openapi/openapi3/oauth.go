package openapi3

import (
	"context"
	"encoding/json"
)

type (
	OAuthFlows struct {
		Extension         `json:",omitempty" yaml:",inline"`
		Implicit          *OAuthFlow `json:"implicit,omitempty" yaml:"implicit,omitempty"`
		Password          *OAuthFlow `json:"password,omitempty" yaml:"password,omitempty"`
		ClientCredentials *OAuthFlow `json:"clientCredentials,omitempty" yaml:"clientCredentials,omitempty"`
		AuthorizationCode *OAuthFlow `json:"authorizationCode,omitempty" yaml:"authorizationCode,omitempty"`
	}

	OAuthFlow struct {
		Extension        `json:",omitempty" yaml:",inline"`
		AuthorizationURL string            `json:"authorizationUrl,omitempty" yaml:"authorizationUrl,omitempty"`
		TokenURL         string            `json:"tokenUrl,omitempty" yaml:"tokenUrl,omitempty"`
		RefreshURL       string            `json:"refreshUrl,omitempty" yaml:"refreshUrl,omitempty"`
		Scopes           map[string]string `json:"scopes" yaml:"scopes"`
	}
)

func (o *OAuthFlows) UnmarshalJSON(b []byte) error {
	type temp OAuthFlows
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*o = OAuthFlows(tmp)
	return o.Extension.UnmarshalJSON(b)
}

func (o *OAuthFlows) MarshalJSON() ([]byte, error) {
	type temp OAuthFlows
	tmp := temp(*o)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(o.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(o.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (o *OAuthFlow) UnmarshalJSON(b []byte) error {
	type temp OAuthFlow
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*o = OAuthFlow(tmp)
	return o.Extension.UnmarshalJSON(b)
}

func (o *OAuthFlow) MarshalJSON() ([]byte, error) {
	type temp OAuthFlow
	tmp := temp(*o)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(o.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(o.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}



func (s *OAuthFlows) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp OAuthFlows
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
	*s = OAuthFlows(tmp)
	return nil
}


func (s *OAuthFlow) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp OAuthFlow
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
	*s = OAuthFlow(tmp)
	return nil
}
