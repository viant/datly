package parser

import (
	"encoding/json"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view/state"
	"strings"
)

type (
	Declaration struct {
		inference.Parameter
		//ParametersState shorthands
		Auth          string            `json:",omitempty" yaml:",omitempty"`
		Kind          string            `json:",omitempty" yaml:",omitempty"`
		Location      *string           `json:",omitempty" yaml:",omitempty"`
		OutputType    string            `json:",omitempty" yaml:",omitempty"`
		Cardinality   state.Cardinality `json:",omitempty" yaml:",omitempty"`
		StatusCode    *int              `json:",omitempty" yaml:",omitempty"`
		ErrorMessage  *string           `json:",omitempty" yaml:",omitempty"`
		TransformKind string            `json:",omitempty" yaml:",omitempty"`
		Transformer   string            `json:",omitempty" yaml:",omitempty"`
		Codec         string            `json:",omitempty" yaml:",omitempty"`
		CodecArgs     []string          `json:",omitempty" yaml:",omitempty"`
		QuerySelector string            `json:",omitempty" yaml:",omitempty"`
		Raw           string
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
		if d.Parameter.Output.Name == "" {
			d.Parameter.Output.Name = d.Codec
		}
		if len(d.Parameter.Output.Args) == 0 {
			d.Parameter.Output.Args = d.CodecArgs
		}

		switch d.Codec { //TODO get codec registry here
		case "JwtClaim":
			required := true
			d.Required = &required
			if d.Output == nil {
				d.Output = &state.Codec{}
			}
			if d.Output.Schema == nil {
				d.Output.Schema = &state.Schema{}
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
		d.Parameter.In.Kind = state.Kind(d.Kind)
		if d.Location != nil {
			d.Parameter.In.Name = *d.Location
		}
	}

	if d.StatusCode != nil {
		d.Parameter.ErrorStatusCode = *d.StatusCode
	}
	if d.ErrorMessage != nil {
		d.Parameter.ErrorMessage = *d.ErrorMessage
	}

	if d.Cardinality != "" {
		d.Parameter.Schema.Cardinality = d.Cardinality
	}

	if d.SQL != "" {
		if d.In.Kind == "" && IsStructQL(d.SQL) {
			d.In.Kind = state.KindParam
			d.In.Name = d.Name
		}
		if d.In.Kind == state.KindParam {
			d.Parameter.EnsureCodec()
			d.Parameter.Output.Body = d.SQL
			d.Parameter.Output.Ref = "structql"
		}
	}

	if d.SQL != "" && d.In.Kind == "" {
		if d.Parameter.Schema.Cardinality == "" {
			d.Parameter.Schema.Cardinality = state.Many
		}
		d.In = state.NewViewLocation(d.Name)
	}
	if d.In != nil && d.In.Kind == state.KindRequestBody {
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
		Parameter: state.Parameter{
			Name:            name,
			In:              &state.Location{Name: "Authorization", Kind: state.KindHeader},
			ErrorStatusCode: 401,
			Required:        &required,
			Output:          &state.Codec{Name: "JwtClaim", Schema: &state.Schema{DataType: "*JwtClaims"}},
			Schema:          &state.Schema{DataType: "string"},
		}}
}
