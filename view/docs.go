package view

import (
	"context"
	"fmt"
	"github.com/viant/xdatly/docs"
	"strings"
)

type (
	Docs struct {
		Name          string
		DefaultPkg    string
		ConnectorName string
		URL           string
		Types         *TypesDoc
		_service      docs.Service
	}

	TypesDoc struct {
		DefaultPkg string
		Doc        []*TypeDoc
		_docIndex  map[string]int
	}

	TypeDoc struct {
		Pkg    string
		Name   string
		Fields map[string]string
	}

	MapBasedDoc struct {
		index map[string]string
	}
)

func (m *MapBasedDoc) Lookup(ctx context.Context, key string) (string, bool, error) {
	result, ok := m.index[key]
	return result, ok, nil
}

func (d *Docs) Init(ctx context.Context, registry *docs.Registry, connectors Connectors) error {
	if d.Types != nil {
		d._service = NewMapBasedDoc(d.Types)
		return nil
	}

	if d.Name == "" {
		return fmt.Errorf("name can't be empty")
	}

	provider := registry.Lookup(d.Name)
	if provider == nil {
		return fmt.Errorf("not found Documentation provider with name %v", d.Name)
	}

	var serviceOptions []docs.Option
	if d.URL != "" {
		serviceOptions = append(serviceOptions, docs.WithURL(d.URL))
	}

	if d.ConnectorName != "" {
		connector, err := connectors.Lookup(d.ConnectorName)
		if err != nil {
			return err
		}

		serviceOptions = append(serviceOptions, docs.WithConnector(connector))
	}

	service, err := provider.Service(ctx, serviceOptions...)
	if err != nil {
		return err
	}

	d._service = service
	return nil
}

func NewMapBasedDoc(types *TypesDoc) docs.Service {
	index := map[string]string{}
	for _, doc := range types.Doc {
		if doc.Pkg == "" {
			doc.Pkg = types.DefaultPkg
		}

		for key, value := range doc.Fields {
			index[strings.Join([]string{doc.Pkg, key}, ".")] = value
		}
	}

	return &MapBasedDoc{
		index: index,
	}
}
