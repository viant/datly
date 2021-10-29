package sql

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"io/ioutil"
)

type Fragment struct {
	Key string `json:",omitempty"`
	SQL string `json:",omitempty"`
	URL string `json:",omitempty"`
}

//LoadSQL loads sql
func (v *Fragment) LoadSQL(ctx context.Context, fs afs.Service, parentURL string) error {
	fromURL := v.URL
	if fromURL == "" {
		return nil
	}
	if url.IsRelative(v.URL) {
		fromURL = url.JoinUNC(parentURL, v.URL)
	}
	reader, err := fs.OpenURL(ctx, fromURL)
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return errors.Wrapf(err, "failed to read: %v", fromURL)
	}
	v.SQL = string(data)
	return nil
}
