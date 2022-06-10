package router

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/view"
)

type Loader struct {
	Ctx  context.Context
	Path string

	types        view.Types
	visitors     codec.Visitors
	metrics      *view.Metrics
	afsService   afs.Service
	dependencies map[string]*view.Resource
}

func NewLoader(ctx context.Context, path string) *Loader {
	return &Loader{
		Ctx:  ctx,
		Path: path,
	}
}

func (l *Loader) SetTypes(types view.Types) *Loader {
	l.types = types
	return l
}

func (l *Loader) SetVisitors(visitors codec.Visitors) *Loader {
	l.visitors = visitors
	return l
}

func (l *Loader) SetMetrics(metrics *view.Metrics) *Loader {
	l.metrics = metrics
	return l
}

func (l *Loader) SetAfsService(service afs.Service) *Loader {
	l.afsService = service
	return l
}

func (l *Loader) SetDependencies(dependencies map[string]*view.Resource) *Loader {
	l.dependencies = dependencies
	return l
}

func (l *Loader) Load() (*Resource, error) {
	return NewResourceFromURL(l.Ctx, l.afsService, l.Path, l.visitors, l.types, l.dependencies, l.metrics)
}
