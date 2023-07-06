package parser

import (
	"encoding/json"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/datly/view"
	"strings"
)

type (
	Declaration struct {
		inference.Parameter
		//Parameters shorthands
		Auth        string           `json:",omitempty" yaml:",omitempty"`
		Kind        string           `json:",omitempty" yaml:",omitempty"`
		Location    *string          `json:",omitempty" yaml:",omitempty"`
		Codec       string           `json:",omitempty" yaml:",omitempty"`
		OutputType  string           `json:",omitempty" yaml:",omitempty"`
		Cardinality view.Cardinality `json:",omitempty" yaml:",omitempty"`
		StatusCode  *int             `json:",omitempty" yaml:",omitempty"`

		TransformKind string `json:",omitempty" yaml:",omitempty"`
		Transformer   string `json:",omitempty" yaml:",omitempty"`
	}
)

func (d *Declaration) Merge(from *Declaration) (*Declaration, error) {
	encoded, err := mergeJsonStructs(d, from)
	if err != nil {
		return nil, err
	}
	result := &Declaration{}
	return result, json.Unmarshal(encoded, result)
}

func (d *Declaration) PathWithName() (string, string) {
	sep := strings.LastIndex(d.Name, ".")
	if sep == -1 {
		return "", d.Name
	}
	return d.Name[:sep], d.Name[sep+1:]
}

func (d *Declaration) Transform() *marshal.Transform {
	aPath, name := d.PathWithName()
	return &marshal.Transform{
		ParamName:   name,
		Kind:        d.TransformKind,
		Path:        aPath,
		Codec:       d.Codec,
		Source:      strings.TrimSpace(d.SQL),
		Transformer: d.Transformer,
	}
}

func (d *Declaration) ExpandShorthands() {
	if d.OutputType != "" || d.Codec != "" {
		d.Parameter.EnsureCodec()
		d.Parameter.Codec.OutputType = d.OutputType
		d.Parameter.Codec.Name = d.Codec
	}

	if d.Kind != "" || d.Location != nil {
		d.EnsureLocation()
		d.Parameter.In.Kind = view.Kind(d.Kind)
		if d.Location != nil {
			d.Parameter.In.Name = *d.Location
		}
	}

	if d.StatusCode != nil {
		d.Parameter.ErrorStatusCode = *d.StatusCode
	}

	if d.Cardinality != "" {
		d.Parameter.EnsureSchema()
		d.Parameter.Schema.Cardinality = d.Cardinality
	}
}

func (d *Declaration) AuthParameter() *inference.Parameter {
	if d.Auth == "" {
		return nil
	}
	required := true
	authParameter := &inference.Parameter{Parameter: view.Parameter{
		Name:            d.Auth,
		In:              &view.Location{Name: "Authorization", Kind: view.KindHeader},
		ErrorStatusCode: 401,
		Required:        &required,
		Output:          &view.Codec{Name: "JwtClaim", Schema: &view.Schema{DataType: "*JwtClaims"}},
		Schema:          &view.Schema{DataType: "string"},
	}}
	if d.ErrorStatusCode != 0 {
		authParameter.ErrorStatusCode = d.ErrorStatusCode
	}
	return authParameter
}
