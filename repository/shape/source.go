package shape

import (
	"reflect"
	"strings"

	"github.com/viant/x"
)

func (e *Engine) structSource(src any) (*Source, error) {
	if src == nil {
		return nil, ErrNilSource
	}
	rType := reflect.TypeOf(src)
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	registry := x.NewRegistry()
	registry.Register(x.NewType(rType))
	return &Source{
		Name:         e.options.Name,
		Struct:       src,
		Type:         rType,
		TypeName:     x.NewType(rType).Key(),
		TypeRegistry: registry,
		DQL:          "",
	}, nil
}

func (e *Engine) dqlSource(dql string) (*Source, error) {
	dql = strings.TrimSpace(dql)
	if dql == "" {
		return nil, ErrNilDQL
	}
	return &Source{
		Name: e.options.Name,
		DQL:  dql,
	}, nil
}
