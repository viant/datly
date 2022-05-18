package config

import (
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/v0/config/rule"
	"github.com/viant/datly/v0/data"
	"golang.org/x/net/context"
	"strings"
)

//Rule represents view rule
type Rule struct {
	Info       rule.Info
	Path       string `json:",omitempty"`
	PathPrefix string `json:",omitempty"`
	UseCache   bool   `json:",omitempty"`
	data.Meta
}

//Validate checks if rule is valid
func (r *Rule) Validate() error {
	if r.Path == "" {
		return errors.Errorf("Path was empty, %v", r.Info.URL)
	}
	if err := r.Meta.Validate(); err != nil {
		return errors.Wrapf(err, "failed to validate rule: %v", r.Info.URL)
	}
	return nil
}

//Init initialise rule
func (r *Rule) Init(ctx context.Context, fs afs.Service) (err error) {
	if r.TemplateURL != "" {
		err = r.initTemplate(ctx, fs)
		if err != nil {
			return errors.Wrapf(err, "failed to initialise template: %v", r.TemplateURL)
		}
	}
	return r.initRule(ctx, fs)
}

func (r *Rule) initTemplate(ctx context.Context, fs afs.Service) error {
	parentURL, _ := url.Split(r.Info.URL, "")
	templateURL := r.TemplateURL
	if url.IsRelative(templateURL) {
		templateURL = url.JoinUNC(parentURL, templateURL)
	}
	rule, err := loadRule(ctx, fs, templateURL)
	if err != nil {
		return err
	}
	r.Meta.SetTemplate(&rule.Meta)
	return nil
}

func (r *Rule) initRule(ctx context.Context, fs afs.Service) error {
	parentURL, _ := url.Split(r.Info.URL, "")
	var err error
	if len(r.Views) > 0 {
		for i := range r.Views {
			if err = r.Views[i].LoadSQL(ctx, fs, parentURL); err != nil {
				return err
			}
			if err := r.Views[i].Init(i > 0); err != nil {
				return err
			}
		}
	}
	r.Meta.ApplyTemplate()
	r.initPathPrefix()
	return r.Meta.Init()
}

func (r *Rule) initPathPrefix() {
	if r.PathPrefix == "" && r.Path != "" {
		uriPrefix := r.Path
		if index := strings.Index(string(uriPrefix[1:]), "{"); index != -1 {
			uriPrefix = string(uriPrefix[:index+1])
		}
		r.PathPrefix = uriPrefix
	}
}
