package config

import (
	"context"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"io/ioutil"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

//Rules stores rules
type Rules struct {
	URL      string
	registry map[string]*Rule
	rules    []*Rule
	Loader   *Loader
}

// Len is the number of elements in the collection.
func (r Rules) Len() int {
	return len(r.rules)
}

// Swap swaps the elements with indexes i and j.
func (r Rules) Swap(i, j int) {
	tmp := r.rules[i]
	r.rules[i] = r.rules[j]
	r.rules[j] = tmp
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (r Rules) Less(i, j int) bool {
	return len(r.rules[i].Path) > len(r.rules[j].Path)
}

//Init initialises rules
func (r *Rules) Init(ctx context.Context, fs afs.Service) error {
	r.registry = make(map[string]*Rule)
	r.Loader = NewLoader(r.URL, time.Second, fs, r.modify, r.remove)
	_, err := r.Loader.Notify(ctx, fs)
	return err
}

//Match matches rule with path
func (r Rules) Match(Path string) (*Rule, url.Values) {
	//TODO optimize matching
	if len(r.rules) == 0 {
		return nil, nil
	}
	values := url.Values{}
	for _, rule := range r.rules {
		if strings.HasPrefix(Path, rule.PathPrefix) {
			params, matched := MatchPath(rule.Path, Path)
			if !matched {
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

//Load load a rule
func (r *Rules) Load(ctx context.Context, fs afs.Service, URL string) error {

	if len(r.rules) == 0 {
		r.rules = make([]*Rule, 0)
	}
	rule, err := loadRule(ctx, fs, URL)
	if err == nil {
		err = rule.Validate()
	}
	if err != nil {
		return err
	}

	r.registry[rule.Info.URL] = rule
	var rules = make([]*Rule, 0)
	for k := range r.registry {
		rules = append(rules, r.registry[k])
	}
	sort.Sort(&Rules{rules: rules})
	r.rules = rules
	return err
}

func loadRule(ctx context.Context, fs afs.Service, URL string) (*Rule, error) {
	reader, err := fs.OpenURL(ctx, URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load resource: %v", URL)
	}
	defer func() {
		_ = reader.Close()
	}()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	var rule *Rule
	err = loadTarget(data, path.Ext(URL), func() interface{} {
		return &Rule{}
	}, func(target interface{}) error {
		rule, _ = target.(*Rule)
		if rule == nil {
			return errors.Errorf("invalid rule type %T", target)
		}
		rule.Info.URL = URL
		return rule.Init(ctx, fs)
	})
	return rule, err
}
