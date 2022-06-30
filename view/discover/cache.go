package discover

import (
	"bytes"
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/datly/view"
	"gopkg.in/yaml.v3"
	"time"
)

type (
	Cache struct {
		Items     map[string]view.Columns
		ModTime   time.Time
		SourceURL string
	}
)

func (c *Cache) Load(ctx context.Context) error {
	fs := afs.New()
	data, err := fs.DownloadWithURL(ctx, c.SourceURL)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(data, c)
}

func (c *Cache) Exists(ctx context.Context) bool {
	fs := afs.New()
	exists, _ := fs.Exists(ctx, c.SourceURL)
	return exists
}

func (c *Cache) Store(ctx context.Context) error {
	fs := afs.New()
	asBytes, err := yaml.Marshal(c)
	if err != nil {
		return err
	}

	return fs.Upload(ctx, c.SourceURL, file.DefaultFileOsMode, bytes.NewReader(asBytes))
}

func (c *Cache) Lookup(viewName string) view.Columns {
	if columns, ok := c.Items[viewName]; ok {
		return columns
	}

	columns := view.Columns{}
	c.Items[viewName] = columns
	return columns
}

func New(sourceURL string) *Cache {
	return &Cache{
		Items:     map[string]view.Columns{},
		SourceURL: sourceURL,
	}
}
