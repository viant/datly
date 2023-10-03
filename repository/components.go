package repository

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/repository/resource"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
)

type Components struct {
	URL         string `json:",omitempty" yaml:",omitempty"`
	Version     version.Control
	With        []string //list of resource to inherit from  `json:",omitempty"`
	Components  []*Component
	Resource    *view.Resource
	columns     *discover.Columns
	resources   *resource.Resources
	options     *Options
	initialized bool
}

func (c *Components) Init(ctx context.Context) error {
	if c.initialized {
		return nil
	}
	c.initialized = true
	if err := c.ensureColumns(ctx); err != nil {
		return err
	}
	if err := c.Resource.Init(ctx, c.options.registry, c.columns.Items); err != nil {
		return err
	}
	for _, component := range c.Components {
		if err := component.Init(ctx, c.Resource); err != nil {
			return err
		}
	}
	return nil
}

func (c *Components) columnsFile() string {
	parent, leaf := url.Split(c.URL, file.Scheme)
	return url.Join(parent, ".meta", leaf)
}

func (c *Components) mergeResources() error {
	if len(c.With) == 0 {
		return nil
	}
	for _, ref := range c.With {
		refResource, err := c.options.resources.Lookup(ref)
		if err != nil {
			return err
		}
		c.Resource.MergeFrom(refResource.Resource, c.options.registry.Types)
	}
	return nil
}

func (c *Components) ensureColumns(ctx context.Context) error {
	if c.columns == nil {
		c.columns = discover.New(c.columnsFile(), c.options.fs)
	}
	if !c.options.useColumns {
		return nil
	}
	if len(c.columns.Items) > 0 {
		return nil
	}
	return c.columns.Load(ctx)
}

func LoadComponents(ctx context.Context, URL string, opts ...Option) (*Components, error) {
	options := NewOptions(opts...)
	data, err := options.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	components, err := unmarshalComponent(data)
	if err != nil {
		return nil, err
	}
	components.URL = URL

	components.options = options
	if components.Resource == nil {
		return nil, fmt.Errorf("components was empty: %v", URL)
	}
	if err = components.mergeResources(); err != nil {
		return nil, err
	}
	components.Resource.Metrics = options.metrics
	components.Resource.SourceURL = URL
	components.Resource.SetTypes(options.registry.Types)
	object, _ := options.fs.Object(ctx, URL)
	components.Resource.ModTime = object.ModTime()
	return components, nil
}

func unmarshalComponent(data []byte) (*Components, error) {
	aMap := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &aMap); err != nil {
		return nil, err
	}
	ensureComponents(aMap)
	components := &Components{}
	err := toolbox.DefaultConverter.AssignConverted(components, aMap)
	if err != nil {
		return nil, err
	}
	return components, err
}

func ensureComponents(aMap map[string]interface{}) {
	if _, ok := aMap["Components"]; !ok { //forward compatibiltiy
		aMap["Components"] = aMap["Routes"]
	}
}
