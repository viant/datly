package config

import (
	"context"
	"datly/base"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"io/ioutil"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

type Rules struct {
	URL      string
	registry map[string]*Rule
	rules    []*Rule
	Loader   *base.Loader
}


// Len is the number of elements in the collection.
func (o Rules) Len() int {
	return len(o.rules)
}

// Swap swaps the elements with indexes i and j.
func (o Rules) Swap(i, j int) {
	tmp := o.rules[i]
	o.rules[i] = o.rules[j]
	o.rules[j] = tmp
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (o Rules) Less(i, j int) bool {
	return  len(o.rules[i].URI) > len(o.rules[j].URI)
}


func (r *Rules) Init(ctx context.Context, fs afs.Service) error {
	r.registry = make(map[string]*Rule)
	r.Loader = base.NewLoader(r.URL, time.Second, fs, r.modify, r.remove)
	_, err := r.Loader.Notify(ctx, fs)
	return err
}


//TODO optimize matching
func (r Rules) Match(URI string) (*Rule, url.Values) {
	if len(r.rules) == 0 {
		return nil, nil
	}
	values := url.Values{}
	for _, rule := range r.rules {
		if strings.HasPrefix(URI, rule.URIPrefix) {
			params, matched := base.MatchURI(rule.URI, URI)
			if ! matched {
				continue
			}
			for k, v := range params {
				values.Add(k, v)
			}
			return rule, values
		}
	}
	return nil, nil
}



func (r *Rules) modify(ctx context.Context, fs afs.Service, URL string) error {
	 err := r.Load(ctx, fs, URL)
	 return err
}

func (r *Rules) remove(ctx context.Context, fs afs.Service, URL string) error {
	delete(r.registry, URL)
	return nil
}


func (r *Rules) Load(ctx context.Context, fs afs.Service, URL string) error {
	reader, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return errors.Wrapf(err, "failed to load resource: %v", URL)
	}
	if len(r.rules) == 0 {
		r.rules = make([]*Rule, 0)
	}



	defer func() {
		_ = reader.Close()
	}()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	err = loadTarget(data, path.Ext(URL), func() interface{} {
		return &Rule{}
	}, func(target interface{}) error {
		rule, ok := target.(*Rule)
		if ! ok {
			return errors.Errorf("invalid rule type %T", target)
		}
		rule.Info.URL = URL
		err := rule.Init(ctx, fs)
		if err == nil {
			if err = rule.Validate(); err == nil {
				r.registry[rule.Info.URL] = rule
			}
		}
		return err
	})
	if err != nil {
		return err
	}
	var rules = make([]*Rule, 0)
	for k := range r.registry {
		rules = append(rules, r.registry[k])
	}
	sort.Sort(&Rules{rules:rules})
	r.rules = rules
	return err
}
