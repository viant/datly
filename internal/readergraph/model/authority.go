// Package model owns immutable checked reader graph associations. Wire metadata
// cannot construct an association; bootstrap admits a current holder/resource closure.
package model

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	selectorModel "github.com/viant/datly/internal/selectorcompanion/model"
	"io/fs"
)

type Authority struct {
	selector      *selectorModel.Association
	contracts     string
	graph         []byte
	input, output string
	assets        map[string][32]byte
}

func New(graph []byte, input, output string, assets map[string][]byte, contracts string) *Authority {
	a := &Authority{graph: bytes.Clone(graph), contracts: contracts, input: input, output: output, assets: map[string][32]byte{}}
	for uri, body := range assets {
		a.assets[uri] = sha256.Sum256(body)
	}
	return a
}
func (a *Authority) Validate(graph []byte, input, output string, resources fs.FS) error {
	if a == nil || !bytes.Equal(a.graph, graph) || input != a.input || output != a.output {
		return fmt.Errorf("reader metadata checked graph or contracts changed")
	}
	if resources == nil {
		return fmt.Errorf("reader metadata resource authority is required")
	}
	for uri, want := range a.assets {
		body, err := fs.ReadFile(resources, uri)
		if err != nil {
			return fmt.Errorf("reader metadata resource %s: %w", uri, err)
		}
		if sha256.Sum256(body) != want {
			return fmt.Errorf("reader metadata resource %s changed after validation", uri)
		}
	}
	return nil
}
func (a *Authority) ValidateGraph(graph []byte, input, output string) error {
	if a == nil || !bytes.Equal(a.graph, graph) || input != a.input || output != a.output {
		return fmt.Errorf("reader metadata checked graph or contracts changed")
	}
	return nil
}
func (a *Authority) Assets() map[string][32]byte {
	r := map[string][32]byte{}
	if a != nil {
		for k, v := range a.assets {
			r[k] = v
		}
	}
	return r
}

func (a *Authority) WithSelector(selector *selectorModel.Association) *Authority {
	copy := *a
	copy.selector = selector
	return &copy
}
func (a *Authority) ValidateSelector(selector *selectorModel.Association) error {
	if a == nil || a.selector != selector {
		return fmt.Errorf("reader metadata selector association changed")
	}
	return nil
}

func (a *Authority) ValidateContracts(signature string) error {
	if a == nil || signature != a.contracts {
		return fmt.Errorf("reader metadata contract closure changed")
	}
	return nil
}
