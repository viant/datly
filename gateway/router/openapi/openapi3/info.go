package openapi3

import (
	"context"
	"encoding/json"
)

// Info represents document info
type (
	Info struct {
		Extension      `json:",omitempty" yaml:",inline"`
		Title          string   `json:"title" yaml:"title"` // Required
		Description    string   `json:"description,omitempty" yaml:"description,omitempty"`
		TermsOfService string   `json:"termsOfService,omitempty" yaml:"termsOfService,omitempty"`
		Contact        *Contact `json:"contact,omitempty" yaml:"contact,omitempty"`
		License        *License `json:"license,omitempty" yaml:"license,omitempty"`
		Version        string   `json:"version" yaml:"version"` // Required
	}
	Contact struct {
		Extension `json:",omitempty" yaml:",inline"`
		Name      string `json:"name,omitempty" yaml:"name,omitempty"`
		URL       string `json:"url,omitempty" yaml:"url,omitempty"`
		Email     string `json:"email,omitempty" yaml:"email,omitempty"`
	}
	License struct {
		Extension `json:",omitempty" yaml:",inline"`
		Name      string `json:"name" yaml:"name"` // Required
		URL       string `json:"url,omitempty" yaml:"url,omitempty"`
	}
)

func (i *Info) UnmarshalJSON(b []byte) error {
	type temp Info
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*i = Info(tmp)
	return i.Extension.UnmarshalJSON(b)
}

func (i *Info) MarshalJSON() ([]byte, error) {
	type temp Info
	tmp := temp(*i)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(i.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(i.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (c *Contact) UnmarshalJSON(b []byte) error {
	type temp Contact
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*c = Contact(tmp)
	return c.Extension.UnmarshalJSON(b)
}

func (c *Contact) MarshalJSON() ([]byte, error) {
	type temp Contact
	tmp := temp(*c)
	tmp.Extension = nil
	data, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	if len(c.Extension) == 0 {
		return data, nil
	}
	extData, err := json.Marshal(c.Extension)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return extData, nil
	}
	res := mergeJSON(data, extData)
	return res, nil
}

func (l *License) UnmarshalJSON(b []byte) error {
	type temp License
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	*l = License(tmp)
	return l.Extension.UnmarshalJSON(b)
}

func (l *License) MarshalJSON() ([]byte, error) {
	type temp License
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

func (i *Info) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Info
	tmp := temp(*i)
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
	*i = Info(tmp)
	return nil
}

func (c *Contact) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Contact
	tmp := temp(*c)
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
	*c = Contact(tmp)
	return nil
}

func (l *License) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp License
	tmp := temp(*l)
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
	*l = License(tmp)
	return nil
}
