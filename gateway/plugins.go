package gateway

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	pgoBuild "github.com/viant/pgo/build"
	"github.com/viant/pgo/manager"
	"github.com/viant/xdatly/types/core"
	"plugin"
	"reflect"
	"sort"
	"time"
)

const TimePluginsLayout = "20060102T150405Z0700"

type (
	pluginDataSlice []*pluginData
	pluginData      struct {
		creationTime time.Time
		packageName  string
		changes      []interface{}
	}

	pluginMetadata struct {
		URL          string
		CreationTime time.Time
		pgoBuild.Info
	}
)

func (p pluginDataSlice) Len() int {
	return len(p)
}

func (p pluginDataSlice) Less(i, j int) bool {
	return p[i].creationTime.Before(p[j].creationTime)
}

func (p pluginDataSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (r *Service) handlePluginsChanges(ctx context.Context, changes *ResourcesChange) (*config.Registry, error) {
	updateSize := len(changes.pluginsIndex.updated)
	if updateSize == 0 {
		return nil, nil
	}
	started := time.Now()
	defer func() {
		fmt.Printf("loaded plugin after: %s\n", time.Since(started))
	}()

	registry := config.NewRegistry()
	var types []string
	_, cancelFn := core.Types(func(packageName, typeName string, rType reflect.Type, _ time.Time) {
		registry.AddType(packageName, typeName, rType)
	})

	defer cancelFn()

	aChan := make(chan func() (*pluginData, error), updateSize)
	for i := 0; i < updateSize; i++ {
		go r.loadPlugin(ctx, changes.pluginsIndex.updated[i], aChan)
	}

	var pluginsData pluginDataSlice
	var i = 0
	for fn := range aChan {
		i++
		if i == updateSize {
			close(aChan)
		}
		data, err := fn()
		if err != nil {
			fmt.Printf("[ERROR] error occured while reading plugin %v\n", err.Error())
			continue
		}
		if data == nil {
			continue
		}
		if len(data.changes) == 0 {
			continue
		}
		pluginsData = append(pluginsData, data)
	}

	sort.Sort(pluginsData)

	for _, pluginChanges := range pluginsData {
		for _, change := range pluginChanges.changes {
			switch actual := change.(type) {
			case *map[string]reflect.Type:
				registry.OverrideTypes(pluginChanges.packageName, *actual)
			case *[]reflect.Type:
				registry.AddTypes(pluginChanges.packageName, *actual)
			case *map[string][]reflect.Type:
				registry.OverridePackageTypes(*actual)
			case *map[string]map[string]reflect.Type:
				registry.OverridePackageNamedTypes(*actual)
			case **config.Registry:
				registry.Override(*actual)
			}
		}
	}

	if len(types) > 0 {
		fmt.Printf("[INFO] detected plugin changes, overriding types %s\n", types)
	}
	config.Config.Override(registry)
	return registry, nil
}

func (r *Service) handlePluginConfig(pluginProvider *plugin.Plugin, data *pluginData) {
	configPlugin, err := pluginProvider.Lookup(config.PluginConfig)
	if err != nil {
		return
	}

	data.changes = append(data.changes, configPlugin)
}

func (r *Service) handlePluginTypes(provider *plugin.Plugin, data *pluginData) {
	types, err := provider.Lookup(config.TypesName)
	if err != nil {
		return
	}
	packageSymbol, err := provider.Lookup(config.PackageName)
	var packageName string
	if err == nil {
		name, ok := packageSymbol.(*string)
		if ok {
			packageName = *name
		}
	}
	data.changes = append(data.changes, types)
	data.packageName = packageName
}

func (r *Service) loadPlugin(ctx context.Context, URL string, aChan chan func() (*pluginData, error)) {
	aData, err := r.loadPluginData(ctx, URL)
	aChan <- func() (*pluginData, error) {
		return aData, err
	}
}

func (r *Service) loadPluginData(ctx context.Context, URL string) (*pluginData, error) {
	//if index := strings.Index(URL, r.Config.DependencyURL); index != -1 {
	//	URL = URL[index:]
	//}
	var reasons []string
	info, pluginProvider, err := r.pluginManager.OpenWithInfoURL(ctx, URL)
	if err != nil {
		if manager.IsPluginOutdated(err) {
			reasons = append(reasons, err.Error())
		} else {
			return nil, err
		}
	}
	createdAt, _ := info.Scn.AsTime()
	aData := &pluginData{
		creationTime: createdAt,
	}
	r.handlePluginConfig(pluginProvider, aData)
	r.handlePluginTypes(pluginProvider, aData)
	return aData, nil
}
