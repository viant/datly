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
	Columns struct {
		fs        afs.Service
		cfs       afs.Service
		Items     map[string]view.Columns
		SourceURL string
		modTime   time.Time `yaml:"-"`
	}
)

func (c *Columns) SetModTime(modTime time.Time) {
	c.modTime = modTime
}

func (c *Columns) ModTime() time.Time {
	return c.modTime
}

func (c *Columns) Load(ctx context.Context) error {
	data, err := c.cfs.DownloadWithURL(ctx, c.SourceURL)
	if err == nil {
		return yaml.Unmarshal(data, c)
	}
	data, err = c.fs.DownloadWithURL(ctx, c.SourceURL)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, c)
}

func (c *Columns) Exists(ctx context.Context) bool {
	exists, _ := c.cfs.Exists(ctx, c.SourceURL)
	if exists {
		return exists
	}
	exists, _ = c.fs.Exists(ctx, c.SourceURL)
	return exists
}

func (c *Columns) Store(ctx context.Context) error {
	sourceURL := c.SourceURL
	c.SourceURL = "" //avoid writing absolute location
	asBytes, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	return c.fs.Upload(ctx, sourceURL, file.DefaultFileOsMode, bytes.NewReader(asBytes))
}

func (c *Columns) Lookup(viewName string) view.Columns {
	if columns, ok := c.Items[viewName]; ok {
		return columns
	}

	columns := view.Columns{}
	c.Items[viewName] = columns
	return columns
}

func New(sourceURL string, cfs afs.Service) *Columns {
	return &Columns{
		fs:        afs.New(),
		cfs:       cfs,
		Items:     map[string]view.Columns{},
		SourceURL: sourceURL,
	}
}
