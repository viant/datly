package config

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/afs"
	afsurl "github.com/viant/afs/url"
	"github.com/viant/datly/bootstrap/connector"
	gateway "github.com/viant/datly/gateway/http"
	"gopkg.in/yaml.v3"
)

type Loader struct {
	FS afs.Service
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
			return nil, fmt.Errorf("invalid local BaseDir URL")
		}
		c.BaseDir = parsed.Path
	}
	if strings.Contains(c.BaseDir, "://") {
		return nil, fmt.Errorf("BaseDir must identify a local authored module")
	}
	for i, dir := range c.ModuleDirs {
		if !filepath.IsAbs(dir) {
			c.ModuleDirs[i] = filepath.Join(c.BaseDir, dir)
		}
	}
	if c.Info != nil {
		if c.OpenAPI != nil {
			return nil, fmt.Errorf("Info and OpenAPI are mutually exclusive")
		}
		c.OpenAPI = &gateway.OpenAPIConfig{Info: *c.Info}
	}
	if c.DependencyURL != "" {
		connectors, err := l.dependencies(ctx, c.DependencyURL)
		if err != nil {
			return nil, err
		}
		c.Connectors = append(c.Connectors, connectors...)
	}
	if c.OpenAPI != nil {
		for i := range c.OpenAPI.StartupExports {
			export := &c.OpenAPI.StartupExports[i]
			if export.URL != "" && afsurl.IsRelative(export.URL) {
				export.URL = afsurl.Join(base, export.URL)
			}
		}
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

func (l Loader) dependencies(ctx context.Context, location string) ([]connector.Config, error) {
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
	var result []connector.Config
	for _, item := range locations {
		var document struct{ Connectors []connector.Config }
		if err := l.decode(ctx, item, &document); err != nil {
			return nil, err
		}
		result = append(result, document.Connectors...)
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
