package plugin

import (
	"context"
	"fmt"
	"github.com/viant/afs/storage"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/view/extension"
	"github.com/viant/pgo/manager"
	"path"
	"plugin"
	"reflect"
	"sync"
	"sync/atomic"
)

type snapshot struct {
	srv      *Service
	changes  int32
	registry *extension.Registry
	mux      sync.Mutex
}

func (s *snapshot) onChange(ctx context.Context, object storage.Object, operation resource.Operation) error {
	ext := path.Ext(object.Name())
	if ext != ".pinf" {
		return nil
	}
	switch operation {
	case resource.Modified, resource.Added:
		atomic.StoreInt32(&s.changes, 1)
		_, aPlugin, err := s.srv.plugins.OpenWithInfoURL(ctx, object.URL())
		if err != nil && !manager.IsPluginOutdated(err) {
			return err
		}
		if err = s.extractExtensions(aPlugin); err != nil {
			return err
		}
	case resource.Deleted:
		return nil
	}
	atomic.StoreInt32(&s.changes, 1)
	return nil
}

func (s *snapshot) extractExtensions(aPlugin *plugin.Plugin) error {
	if err := s.extractExtensionRegistry(aPlugin); err != nil {
		return err
	}
	return s.extractTypesRegistry(aPlugin)
}

func (s *snapshot) extractTypesRegistry(aPlugin *plugin.Plugin) error {
	typesSymbol, err := aPlugin.Lookup(extension.TypesName)
	if err != nil {
		return nil
	}
	packageSymbol, err := aPlugin.Lookup(extension.PackageName)
	var packageName string
	if err == nil {
		name, ok := packageSymbol.(*string)
		if ok {
			packageName = *name
		}
	}
	types, ok := typesSymbol.(*[]reflect.Type)
	if !ok {
		return fmt.Errorf("invalid plugin type: expected %T, but had: %T", types, typesSymbol)
	}
	s.mux.Lock()
	defer s.mux.Unlock()

	s.registry.AddTypes(packageName, *types)
	return nil
}

func (s *snapshot) changed() bool {
	return s.changes > 0
}

func (s *snapshot) extractExtensionRegistry(aPlugin *plugin.Plugin) error {
	registrySymbol, err := aPlugin.Lookup(extension.PluginConfig)
	if err != nil {
		return nil
	}
	registry, ok := registrySymbol.(**extension.Registry)
	if !ok {
		return fmt.Errorf("invalid plugin type: expected %T, but had: %T", registry, registrySymbol)
	}
	s.mux.Lock()
	defer s.mux.Unlock()
	s.registry.MergeFrom(*registry)
	return nil
}

func newSnapshot(srv *Service) *snapshot {
	return &snapshot{srv: srv, registry: extension.NewRegistry()}
}
