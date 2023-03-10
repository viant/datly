package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/build"
	"github.com/viant/datly/config"
	"github.com/viant/xdatly/types/core"
	"os"
	"path"
	"plugin"
	"reflect"
	"sort"
	"strconv"
	"strings"
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

func (r *Service) copyToPluginLoadLocation(ctx context.Context, src string) (string, error) {
	suffix := strconv.Itoa(int(time.Now().UnixNano()))
	loadPluginDir := path.Join(os.TempDir(), "plugins", suffix)
	loadPluginLocation := path.Join(loadPluginDir, path.Base(src))
	return loadPluginLocation, r.fs.Copy(ctx, src, loadPluginLocation)
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

func (r *Service) loadPluginMetadata(ctx context.Context, URL string) (*config.Metadata, error) {
	fileName := path.Base(URL)
	if ext := path.Ext(fileName); ext != "" {
		fileName = strings.ReplaceAll(fileName, ext, "")
	}

	pluginsMetadata := &config.Metadata{
		URL: URL,
	}

	segments := strings.Split(fileName, "_")
	if len(segments) > 1 {
		parsedTime, err := time.Parse(TimePluginsLayout, segments[1])
		if err == nil {
			pluginsMetadata.CreationTime = parsedTime
		}
	}

	if len(segments) > 2 {
		pluginsMetadata.Version = segments[2]
		return pluginsMetadata, nil
	}

	metadataURL := URL + ".meta"
	content, err := r.fs.DownloadWithURL(ctx, metadataURL)
	if err != nil {
		return pluginsMetadata, nil
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

	isOutdated := build.BuildTime.After(metadata.CreationTime)
	goVersionDiff := metadata.Version != "" && metadata.Version != build.GoVersion
	if isOutdated || goVersionDiff {
		var reasons []string
		if isOutdated {
			reasons = append(reasons, "plugin was built before datly")
		}

		if goVersionDiff {
			reasons = append(reasons, fmt.Sprintf("go vesion is different, wanted %v got %v", build.GoVersion, metadata.Version))
		}

		fmt.Printf("[INFO] Ignoring plugin due to the: %v\n", strings.Join(reasons, " | "))
		go r.fs.Delete(context.Background(), metadata.URL)
		go r.fs.Delete(context.Background(), URL)
		return nil, nil
	}

	URL, err = r.copyToPluginLoadLocation(ctx, URL)
	if err != nil {
		return nil, err
	}

	fmt.Printf("[INFO] opening plugin %v\n", URL)
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
