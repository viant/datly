package contract

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"gopkg.in/yaml.v3"
)

type (
	Header struct {
		URL       string
		Resource  *Resource       `yaml:"Resource"`
		Contracts []*ContractPath `yaml:"Routes"`
	}

	Type struct {
		Parameters []*state.Parameter `yaml:"Parameters"`
	}

	Output struct {
		Type *Type `yaml:"Type"`
	}

	ContractPath struct {
		Method string  `yaml:"Method"`
		URI    string  `yaml:"URI"`
		Output *Output `yaml:"Output"`
	}

	Resource struct {
		Types []*view.TypeDefinition `yaml:"Types"`
	}
)

func (h *Header) Signature(contract *ContractPath) *Signature {
	if contract.Output == nil || contract.Output.Type == nil || len(contract.Output.Type.Parameters) == 0 {
		return nil
	}
	parameters := state.Parameters(contract.Output.Type.Parameters)
	outputParameter := parameters.LookupByLocation(state.KindOutput, "data")

	signature := &Signature{Output: outputParameter.Schema, URI: contract.URI, Method: contract.Method}
	for _, candidate := range h.Resource.Types {
		if candidate.Name == outputParameter.Schema.Name {
			signature.Types = append(signature.Types, candidate)
			break
		}
	}
	return signature
}

var fs = afs.New()

func NewHeader(ctx context.Context, URL string) (*Header, error) {
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	ret := &Header{}
	if err = yaml.Unmarshal(data, ret); err != nil {
		return nil, err
	}
	ret.URL = URL
	return ret, nil
}
