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
		Auth          string           `json:",omitempty" yaml:",omitempty"`
		Kind          string           `json:",omitempty" yaml:",omitempty"`
		Location      *string          `json:",omitempty" yaml:",omitempty"`
		Codec         string           `json:",omitempty" yaml:",omitempty"`
		OutputType    string           `json:",omitempty" yaml:",omitempty"`
		Cardinality   view.Cardinality `json:",omitempty" yaml:",omitempty"`
		StatusCode    *int             `json:",omitempty" yaml:",omitempty"`
		TransformKind string           `json:",omitempty" yaml:",omitempty"`
		Transformer   string           `json:",omitempty" yaml:",omitempty"`
	}
)

func (d *Declaration) Merge(from *Declaration) (*Declaration, error) {
	encoded, err := MergeStructs(d, from)
	if err != nil {
		return nil, err
	}
	result := &Declaration{}
	err = json.Unmarshal(encoded, result)
	return result, err
}

func (d *Declaration) PathWithName() (string, string) {
	sep := strings.LastIndex(d.Name, ".")
	if sep == -1 {
		return "", d.Name
	}
	return d.Name[:sep], d.Name[sep+1:]
}

func (d *Declaration) Transform() *marshal.Transform {
	name, aPath := d.PathWithName()
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
	d.Parameter.EnsureSchema()
	d.Parameter.EnsureLocation()
	if d.Required != nil {
		d.Parameter.Parameter.Required = d.Required
	}
	if d.OutputType != "" || d.Codec != "" {
		d.Parameter.EnsureCodec()
		if d.Parameter.Output.OutputType == "" {
			d.Parameter.Output.OutputType = d.OutputType
		}
		if d.Parameter.Output.Name == "" {
			d.Parameter.Output.Name = d.Codec
		}

		switch d.Codec { //TODO get codec registry here
		case "JwtClaim":
			required := true
			d.Required = &required
			if d.Output == nil {
				d.Output = &view.Codec{}
			}
			if d.Output.Schema == nil {
				d.Output.Schema = &view.Schema{}
			}
			if d.Schema.DataType == "" {
				d.Schema.DataType = "string"
			}
			if d.Output.Schema.DataType == "" {
				d.Output.Schema.DataType = "*JwtClaims"
			}
		}

	}

	if d.Kind != "" || d.Location != nil {
		d.Parameter.In.Kind = view.Kind(d.Kind)
		if d.Location != nil {
			d.Parameter.In.Name = *d.Location
		}
	}

	if d.StatusCode != nil {
		d.Parameter.ErrorStatusCode = *d.StatusCode
	}

	if d.Cardinality != "" {
		d.Parameter.Schema.Cardinality = d.Cardinality
	}

	if d.SQL != "" {
		if d.In.Kind == "" && IsStructQL(d.SQL) {
			d.In.Kind = view.KindParam
			d.In.Name = d.Name
		}
		if d.In.Kind == view.KindParam {
			d.Parameter.EnsureCodec()
			d.Parameter.Output.Query = d.SQL
			d.Parameter.Output.Ref = "structql"
		}
	}

	if d.SQL != "" && d.In.Kind == "" {
		if d.Parameter.Schema.Cardinality == "" {
			d.Parameter.Schema.Cardinality = view.Many
		}
		d.In.Kind = view.KindDataView
		d.In.Name = d.Name

	}
	if d.In != nil && d.In.Kind == view.KindRequestBody {
		required := true
		d.Parameter.Required = &required
	}
}

func (d *Declaration) AuthParameter() *inference.Parameter {
	if d.Auth == "" {
		return nil
	}
	authParameter := DefaultOAuthParameter(d.Auth)
	if d.ErrorStatusCode != 0 {
		authParameter.ErrorStatusCode = d.ErrorStatusCode
	}
	return authParameter
}

// DefaultOAuthParameter creates a default oauht parameter
func DefaultOAuthParameter(name string) *inference.Parameter {
	required := true
	return &inference.Parameter{
		Parameter: view.Parameter{
			Name:            name,
			In:              &view.Location{Name: "Authorization", Kind: view.KindHeader},
			ErrorStatusCode: 401,
			Required:        &required,
			Output:          &view.Codec{Name: "JwtClaim", Schema: &view.Schema{DataType: "*JwtClaims"}},
			Schema:          &view.Schema{DataType: "string"},
		}}
}
