package config

import (
	"context"
	"datly/base"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"io/ioutil"
	"path"
	"strings"
	"time"
)

//Connectors represents connectors pool
type Connectors struct {
	registry map[string]*Connector
	URL      string
	Loader   *base.Loader
}

//Get returns a connector for supplied name
func (c Connectors) Get(name string) (*Connector, error) {
	result, ok := c.registry[name]
	if !ok {
		return nil, errors.Errorf("failed to lookup connector for %v", name)
	}
	return result, nil
}

//Init initialises connector
func (c *Connectors) Init(ctx context.Context, fs afs.Service) error {
	c.registry = make(map[string]*Connector)
	c.Loader = base.NewLoader(c.URL, time.Second, fs, c.modify, c.remove)
	_, err := c.Loader.Notify(ctx, fs)
	return err
}

func (c *Connectors) modify(ctx context.Context, fs afs.Service, URL string) error {
	err := c.Load(ctx, fs, URL)
	return err
}

func (c *Connectors) remove(ctx context.Context, fs afs.Service, URL string) error {
	delete(c.registry, URL)
	return nil
}

//Load load connector
func (c *Connectors) Load(ctx context.Context, fs afs.Service, URL string) error {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return errors.Wrapf(err, "failed to load resource: %v", URL)
	}
	defer func() {
		_ = reader.Close()
	}()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	return loadTarget(data, path.Ext(URL), func() interface{} {
		return &Connector{}
	}, func(target interface{}) error {
		connector, ok := target.(*Connector)
		if !ok {
			return errors.Errorf("invalid connector type %T", target)
		}
		connector.URL = URL
		if connector.Name == "" {
			connector.Name = extractBasicName(URL)
		}
		if err = connector.Validate(); err == nil {
			c.registry[connector.Name] = connector
		}
		return err
	})

}

func extractBasicName(URL string) string {
	_, name := url.Split(URL, "")
	if index := strings.Index(name, "."); index != -1 {
		name = string(name[:index])
	}
	return name
}
