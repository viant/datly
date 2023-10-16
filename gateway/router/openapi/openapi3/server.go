package openapi3

import (
	"context"
	"encoding/json"
)

// Server  represents a server node defined is specified by OpenAPI/Swagger standard version 3.0.
type (
	Servers []Server
	Server  struct {
		Extension   `json:",omitempty" yaml:",inline"`
		URL         string                    `json:"url" yaml:"url"`
		Description string                    `json:"description,omitempty" yaml:"description,omitempty"`
		Variables   map[string]ServerVariable `json:"variables,omitempty" yaml:"variables,omitempty"`
	}
	//ServerVariable represents server variables defined by OpenAPI/Swagger standard version 3.0.
	ServerVariable struct {
		Extension   `json:",omitempty" yaml:",inline"`
		Enum        []string `json:"enum,omitempty" yaml:"enum,omitempty"`
		Default     string   `json:"default,omitempty" yaml:"default,omitempty"`
		Description string   `json:"description,omitempty" yaml:"description,omitempty"`
	}
)

func (s *Server) UnmarshalJSON(b []byte) error {
	type temp Server
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*s = Server(tmp)
	return s.Extension.UnmarshalJSON(b)
}

func (s *Server) MarshalJSON() ([]byte, error) {
	type temp Server
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

func (s *Server) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Server
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
	*s = Server(tmp)
	return nil
}

func (s *ServerVariable) UnmarshalJSON(b []byte) error {
	type temp ServerVariable
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*s = ServerVariable(tmp)
	return s.Extension.UnmarshalJSON(b)
}

func (s *ServerVariable) MarshalJSON() ([]byte, error) {
	type temp ServerVariable
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

func (s *ServerVariable) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp ServerVariable
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
	*s = ServerVariable(tmp)
	return nil
}
