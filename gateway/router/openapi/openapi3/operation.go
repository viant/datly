package openapi3

import (
	"context"
	"encoding/json"
)

type Operation struct {
	Extension `json:",omitempty" yaml:",inline"`

	// Optional tags for documentation.
	Tags []string `json:"tags,omitempty" yaml:"tags,omitempty"`

	// Optional short summary.
	Summary string `json:"summary,omitempty" yaml:"summary,omitempty"`

	// Optional description. Should use CommonMark syntax.
	Description string `json:"description,omitempty" yaml:"description,omitempty"`

	// Optional operation ID.
	OperationID string `json:"operationId,omitempty" yaml:"operationId,omitempty"`

	// Optional parameters.
	Parameters Parameters `json:"parameters,omitempty" yaml:"parameters,omitempty"`

	// Optional body parameter.
	RequestBody *RequestBody `json:"requestBody,omitempty" yaml:"requestBody,omitempty"`

	// Responses.
	Responses Responses `json:"responses" yaml:"responses"` // Required

	// Optional callbacks
	Callbacks Callbacks `json:"callbacks,omitempty" yaml:"callbacks,omitempty"`

	Deprecated bool `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`

	// Optional security requirements that overrides top-level security.
	Security *SecurityRequirements `json:"security,omitempty" yaml:"security,omitempty"`

	// Optional servers that overrides top-level servers.
	Servers *Servers `json:"servers,omitempty" yaml:"servers,omitempty"`

	ExternalDocs *ExternalDocumentation `json:"externalDocs,omitempty" yaml:"externalDocs,omitempty"`
}

func (o *Operation) UnmarshalJSON(b []byte) error {
	type temp Operation
	var tmp = temp{}
	err := json.Unmarshal(b, &tmp)
	if err != nil {
		return err
	}
	if tmp.Responses == nil {
		tmp.Responses = Responses{}
	}
	*o = Operation(tmp)
	return o.Extension.UnmarshalJSON(b)
}

func (o *Operation) MarshalJSON() ([]byte, error) {
	type temp Operation
	tmp := temp(*o)
	tmp.Extension = nil
	if tmp.Responses == nil {
		tmp.Responses = Responses{}
	}
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

func (o *Operation) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	type temp Operation
	tmp := temp(*o)
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
	if tmp.Responses == nil {
		tmp.Responses = Responses{}
	}
	*o = Operation(tmp)
	return nil
}
