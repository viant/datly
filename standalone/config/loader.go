package config

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/constant"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/afs"
	afsurl "github.com/viant/afs/url"
	"github.com/viant/datly/bootstrap/connector"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"gopkg.in/yaml.v3"
)

type Loader struct {
	// ConstURL overrides the config file selection; its relative path is process-relative.
	ConstURL string
	FS       afs.Service
	// StaticLocalRoot grants an independently selected, caller-owned authority for
	// local ContentURL paths. Such paths stay relative to this handle, not the
	// config file. The caller keeps it open for the server's reload lifetime.
	StaticLocalRoot *os.Root
}

func (l Loader) Load(ctx context.Context, location string) (*Config, error) {
	if ctx == nil {
		return nil, fmt.Errorf("configuration context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(location) == "" {
		return nil, fmt.Errorf("configuration URL is required")
	}
	if l.FS == nil {
		l.FS = afs.New()
	}
	if !strings.Contains(location, "://") {
		var err error
		location, err = filepath.Abs(location)
		if err != nil {
			return nil, err
		}
	}
	c := &Config{}
	if err := l.decode(ctx, location, c); err != nil {
		return nil, err
	}
	c.URL = location
	c.StaticLocalRoot = l.StaticLocalRoot
	base, _ := afsurl.Split(location, "file")
	if !strings.Contains(location, "://") {
		base = filepath.Dir(location)
	}
	constantURL := c.ConstURL
	if constantURL != "" && afsurl.IsRelative(constantURL) {
		constantURL = afsurl.Join(base, constantURL)
	}
	if l.ConstURL != "" {
		constantURL = l.ConstURL
	}
	var constErr error
	c.Const, constErr = (constant.Loader{FS: l.FS}).Load(ctx, constantURL)
	if constErr != nil {
		return nil, constErr
	}
	if c.Const == nil {
		if err := l.normalize(c); err != nil {
			return nil, err
		}
	}
	if c.Info != nil {
		if c.OpenAPI != nil {
			return nil, fmt.Errorf("Info and OpenAPI are mutually exclusive")
		}
		c.OpenAPI = &gateway.OpenAPIConfig{Info: *c.Info}
	}
	if c.DependencyURL != "" {
		access, err := c.ResolveConstants()
		if err != nil {
			return nil, err
		}
		dependencies, err := l.dependencies(ctx, access.DependencyURL)
		if err != nil {
			return nil, err
		}
		c.Connectors = append(c.Connectors, dependencies.Connectors...)
		c.CacheProviders = append(c.CacheProviders, dependencies.CacheProviders...)
	}
	var err error
	c.Caches, err = namedCaches(c.Caches, c.CacheProviders)
	if err != nil {
		return nil, err
	}
	c.CacheProviders = nil
	resolved, err := c.ResolveConstants()
	if err != nil {
		return nil, err
	}
	if err := resolved.validateCaches(); err != nil {
		return nil, err
	}
	return c, nil
}

func (l Loader) decode(ctx context.Context, location string, target any) error {
	data, err := l.FS.DownloadWithURL(ctx, location)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("download standalone configuration failed")
	}
	parsed, err := url.Parse(location)
	if err != nil {
		return fmt.Errorf("invalid configuration URL")
	}
	ext := strings.ToLower(filepath.Ext(parsed.Path))
	if ext == ".yaml" || ext == ".yml" {
		var document map[string]any
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		if err = decoder.Decode(&document); err != nil {
			return fmt.Errorf("invalid YAML configuration")
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			return fmt.Errorf("configuration must contain one document")
		}
		data, err = json.Marshal(document)
		if err != nil {
			return fmt.Errorf("YAML configuration must use string mapping keys")
		}
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return fmt.Errorf("configuration must be an object")
	}
	if err := validateJSONKeys(data, reflect.TypeOf(target)); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	// Decoder diagnostics may include configured credentials. Do not expose them.
	if err = decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid configuration: unknown field or invalid value")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return fmt.Errorf("configuration must contain one document")
	}
	return ctx.Err()
}

type dependencyDocument struct {
	Connectors     []connector.Config
	Caches         map[string]*spec.CacheSettings
	CacheProviders []*CacheProvider
	ModTime        string // legacy dependency document metadata
}

func (l Loader) dependencies(ctx context.Context, location string) (*dependencyDocument, error) {
	object, err := l.FS.Object(ctx, location)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("read DependencyURL failed")
	}
	locations := []string{location}
	if object.IsDir() {
		objects, err := l.FS.List(ctx, location)
		if err != nil {
			return nil, fmt.Errorf("list DependencyURL failed")
		}
		locations = nil
		for _, item := range objects {
			if item.IsDir() {
				continue
			}
			ext := strings.ToLower(filepath.Ext(item.Name()))
			if ext == ".json" || ext == ".yaml" || ext == ".yml" {
				locations = append(locations, item.URL())
			}
		}
		sort.Strings(locations)
	}
	result := &dependencyDocument{}
	for _, item := range locations {
		var document dependencyDocument
		if err := l.decode(ctx, item, &document); err != nil {
			return nil, err
		}
		result.Connectors = append(result.Connectors, document.Connectors...)
		caches, err := namedCaches(document.Caches, document.CacheProviders)
		if err != nil {
			return nil, err
		}
		for _, name := range cacheNames(caches) {
			settings := caches[name]
			result.CacheProviders = append(result.CacheProviders, &CacheProvider{CacheSettings: *settings, Enabled: &settings.Enabled})
		}
	}
	return result, nil
}

// staticURL preserves local path components for the root owner's no-follow
// validation. Remote URL joining retains AFS semantics.
func (l Loader) staticURL(base, location string) string {
	if l.StaticLocalRoot != nil || !afsurl.IsRelative(location) {
		return location
	}
	parsed, err := url.Parse(base)
	if err == nil && (parsed.Scheme == "" || parsed.Scheme == "file") {
		return strings.TrimRight(base, "/") + "/" + location
	}
	return afsurl.Join(base, location)
}

// normalize applies existing URL/base-directory policy to access arguments.
// Expansion callers invoke this only on a detached, already-expanded config.
func (l Loader) normalize(c *Config) error {
	base, _ := afsurl.Split(c.URL, "file")
	if !strings.Contains(c.URL, "://") {
		base = filepath.Dir(c.URL)
	}
	for _, value := range []*string{&c.RouteURL, &c.PluginsURL, &c.DependencyURL, &c.JobURL, &c.FailedJobURL} {
		if *value != "" && afsurl.IsRelative(*value) {
			*value = afsurl.Join(base, *value)
		}
	}
	if c.Jobs != nil && c.Jobs.Notification.Destination != "" && afsurl.IsRelative(c.Jobs.Notification.Destination) {
		c.Jobs.Notification.Destination = afsurl.Join(base, c.Jobs.Notification.Destination)
	}
	if c.ContentURL != "" {
		c.ContentURL = l.staticURL(base, c.ContentURL)
	}
	if c.ContentURL == "" {
		for _, content := range c.StaticContent {
			if content != nil && content.ContentURL != "" && afsurl.IsRelative(content.ContentURL) {
				content.ContentURL = l.staticURL(base, content.ContentURL)
			}
		}
	}
	if c.BaseDir == "" {
		c.BaseDir = base
	} else if afsurl.IsRelative(c.BaseDir) {
		c.BaseDir = afsurl.Join(base, c.BaseDir)
	}
	if strings.HasPrefix(c.BaseDir, "file://") {
		parsed, err := url.Parse(c.BaseDir)
		if err != nil || parsed.Host != "" && parsed.Host != "localhost" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("invalid local BaseDir URL")
		}
		c.BaseDir = parsed.Path
	}
	if strings.Contains(c.BaseDir, "://") {
		return fmt.Errorf("BaseDir must identify a local authored module")
	}
	for i, dir := range c.ModuleDirs {
		if !filepath.IsAbs(dir) {
			c.ModuleDirs[i] = filepath.Join(c.BaseDir, dir)
		}
	}
	if c.OpenAPI != nil {
		for i := range c.OpenAPI.StartupExports {
			export := &c.OpenAPI.StartupExports[i]
			if export.URL != "" && afsurl.IsRelative(export.URL) {
				export.URL = afsurl.Join(base, export.URL)
			}
		}
	}
	return nil
}
