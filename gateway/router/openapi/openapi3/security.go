package openapi3

import (
	"context"
	"encoding/json"
)

type (
	SecurityRequirements []SecurityRequirement
	SecurityRequirement  map[string][]string
	SecuritySchemes      map[string]*SecurityScheme

	SecurityScheme struct {
		Extension        `json:",omitempty" yaml:",inline"`
		Ref              string      `json:"$ref,omitempty" yaml:"$ref,omitempty"`
		Type             string      `json:"type,omitempty" yaml:"type,omitempty"`
		Description      string      `json:"description,omitempty" yaml:"description,omitempty"`
		Name             string      `json:"name,omitempty" yaml:"name,omitempty"`
		In               string      `json:"in,omitempty" yaml:"in,omitempty"`
		Scheme           string      `json:"scheme,omitempty" yaml:"scheme,omitempty"`
		BearerFormat     string      `json:"bearerFormat,omitempty" yaml:"bearerFormat,omitempty"`
		Flows            *OAuthFlows `json:"flows,omitempty" yaml:"flows,omitempty"`
		OpenIdConnectUrl string      `json:"openIdConnectUrl,omitempty" yaml:"openIdConnectUrl,omitempty"`
	}
)

func (s *SecurityScheme) UnmarshalJSON(b []byte) error {
	type temp SecurityScheme
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*s = SecurityScheme(tmp)
	return s.Extension.UnmarshalJSON(b)
}

func (s *SecurityScheme) MarshalJSON() ([]byte, error) {
	type temp SecurityScheme
	tmp := temp(*s)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(s.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(s.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (s *SecurityScheme) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp SecurityScheme
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
	*s = SecurityScheme(tmp)
	if tmp.Ref == "" {
		return nil
	}
	session := LookupSession(ctx)
	param, err := session.LookupSecurityScheme(session.Location, tmp.Ref)
	if err == nil {
		*s = *param
	}
	return err
}
