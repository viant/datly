package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	furl "github.com/viant/afs/url"
	"github.com/viant/datly/cmd/build"
	"github.com/viant/datly/config"
	"github.com/viant/datly/plugins"
	"github.com/viant/datly/xregistry/types/core"
	"os"
	"path"
	"plugin"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

type (
	pluginDataSlice []*pluginData
	pluginData      struct {
		creationTime time.Time
		packageName  string
		changes      []interface{}
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

func (r *Service) handlePluginsChanges(ctx context.Context, changes *ResourcesChange) (*plugins.Registry, error) {
	updateSize := len(changes.pluginsIndex.updated)
	if updateSize == 0 {
		return nil, nil
	}

	registry := plugins.NewRegistry()
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
			fmt.Printf("[WARN] error occured while reading plugin %v\n", err.Error())
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
			case **plugins.Registry:
				registry.Override(*actual)
			}
		}
	}

	config.Config.Override(registry)

	return registry, nil
}

func (r *Service) handlePluginConfig(pluginProvider *plugin.Plugin, data *pluginData) {
	configPlugin, err := pluginProvider.Lookup(plugins.PluginConfig)
	if err != nil {
		return
	}

	data.changes = append(data.changes, configPlugin)
}

func (r *Service) copyIfNeeded(ctx context.Context, URL string) (string, error) {
	oldURL := URL
	suffix := strconv.Itoa(int(time.Now().UnixNano()))

	if urlScheme := furl.Scheme(URL, ""); urlScheme == "mem" {
		dir := path.Join(os.TempDir(), "plugins", suffix)
		URL = furl.Join(dir, path.Base(URL))
	} else {
		URL = strings.Replace(URL, r.Config.DependencyURL, r.pluginDst(), 1)
		ext := path.Ext(URL)
		URL = strings.Replace(URL, ext, suffix+ext, 1)
	}

	return URL, r.fs.Copy(ctx, oldURL, URL)
}

func (r *Service) pluginDst() string {
	return path.Join(r.Config.DependencyURL, pluginsFolder)
}

func (r *Service) handlePluginTypes(provider *plugin.Plugin, data *pluginData) {
	types, err := provider.Lookup(plugins.TypesName)
	if err != nil {
		return
	}

	packageSymbol, err := provider.Lookup(plugins.PackageName)
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

func (r *Service) loadPluginMetadata(ctx context.Context, URL string) (*plugins.Metadata, error) {
	metadataURL := URL + ".meta"
	content, err := r.fs.DownloadWithURL(ctx, metadataURL)
	if err != nil {
		return nil, err
	}

	pluginsMetadata := &plugins.Metadata{
		URL: metadataURL,
	}
	return pluginsMetadata, json.Unmarshal(content, pluginsMetadata)
}

func (r *Service) loadPlugin(ctx context.Context, URL string, aChan chan func() (*pluginData, error)) {
	aData, err := r.loadPluginWithErr(ctx, URL)
	aChan <- func() (*pluginData, error) {
		return aData, err
	}
}

func (r *Service) loadPluginWithErr(ctx context.Context, URL string) (*pluginData, error) {
	if index := strings.Index(URL, r.Config.DependencyURL); index != -1 {
		URL = URL[index:]
	}

	metadata, err := r.loadPluginMetadata(ctx, URL)
	if err != nil {
		return nil, err
	}

	if build.BuildTime.After(metadata.CreationTime) {
		go r.fs.Delete(context.Background(), metadata.URL)
		go r.fs.Delete(context.Background(), URL)
		return nil, nil
	}

	URL, err = r.copyIfNeeded(ctx, URL)
	if err != nil {
		return nil, err
	}

	pluginProvider, err := plugin.Open(URL)
	if err != nil {
		return nil, err
	}

	aData := &pluginData{
		creationTime: metadata.CreationTime,
	}

	r.handlePluginConfig(pluginProvider, aData)
	r.handlePluginTypes(pluginProvider, aData)

	return aData, nil
}
