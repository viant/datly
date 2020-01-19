package config

import (
	"datly/base"
	"datly/config/rule"
	"datly/data"
	"github.com/go-errors/errors"
	"github.com/viant/afs"
	"golang.org/x/net/context"
	"io/ioutil"
	"strings"
)

//Rules represents data rule
type Rule struct {
	Info      rule.Info
	URI       string `json:",omitempty"`
	URIPrefix string `json:",omitempty"`
	UseCache  bool   `json:",omitempty"`
	data.Meta
}

func (r *Rule) Validate() error {
	if r.URI == "" {
		return errors.Errorf("URI was empty, %v", r.Info.URL)
	}
	return r.Meta.Validate()
}



func (r *Rule) Init(ctx context.Context, fs afs.Service) (err error) {
	if len(r.Views) > 0 {
		for i, view := range r.Views {
			if view.FromURL != "" && view.From == "" {
				view.From, err = loadAsset(fs, ctx, view.FromURL)
				if err != nil {
					return err
				}
			}
			view.Init()
			if i > 0 && view.Selector.Prefix == "" {
				view.Selector.Prefix = view.Name
			}
		}
	}

	if r.URIPrefix == "" && r.URI != "" {
		uriPrefix := r.URI
		if index := strings.Index(string(uriPrefix[1:]), "{"); index != -1 {
			uriPrefix = string(uriPrefix[:index+1])
		}
		r.URIPrefix = uriPrefix
	}

	if len(r.Output) == 0 && len(r.Views) > 0 {
		key := r.Views[0].Table
		if key == "" {
			if key = r.Views[0].Name; key == "" {
				key = base.DefaultDataOutputKey
			}
		}
		r.Output = []*data.Output{
			{
				DataView: r.Views[0].Name,
				Key:      key,
			},
		}
	}

	if len(r.Output) > 0 {
		for i := range r.Output {
			r.Output[i].Init()
		}
	}
	return r.Meta.Init()
}

func loadAsset(fs afs.Service, ctx context.Context, URL string) (string, error) {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	sql, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(sql), err
}
