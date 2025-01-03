package view

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"path"
)

type (
	Documentation struct {
		BaseURL     string
		URLs        []string
		*state.Docs `json:"-"`
	}
)

func (d *Documentation) Init(ctx context.Context, fs afs.Service, substitutes Substitutes) error {
	d.Docs = &state.Docs{}
	for _, URL := range d.URLs {
		if url.IsRelative(URL) {
			URL = path.Join(d.BaseURL, URL)
		}
		var dest = &state.Docs{}
		if err := d.loadDocument(ctx, fs, substitutes, URL, dest); err != nil {
			return err
		}
		d.Docs.Merge(dest)
	}
	return nil
}

func (d *Documentation) loadDocument(ctx context.Context, fs afs.Service, substitutes Substitutes, URL string, dest *state.Docs) error {
	if fs == nil {
		fs = afs.New()
	}
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return err
	}
	data = []byte(substitutes.Replace(string(data)))
	err = shared.UnmarshalWithExt(data, dest, path.Ext(URL))
	return err
}

// NewDocumentation creates a new documentation
func NewDocumentation(URLS ...string) *Documentation {
	return &Documentation{
		URLs: URLS,
		Docs: &state.Docs{},
	}
}
