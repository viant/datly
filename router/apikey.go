package router

import (
	"context"
	"github.com/viant/scy"
	"sort"
	"strings"
)

type (
	APIKey struct {
		URI    string
		Value  string
		Header string
		Secret *scy.Resource
	}

	APIKeys []*APIKey
)

func (k *APIKey) Init() error {
	if k.Secret != nil {
		srv := scy.New()
		secret, err := srv.Load(context.Background(), k.Secret)
		if err != nil {
			return err
		}
		k.Value = secret.String()
	}
	return nil
}

func (a APIKeys) Match(URI string) *APIKey {
	//a needs to be sorted by longest URI
	for _, candidate := range a {
		if candidate.URI == "" || strings.HasPrefix(URI, candidate.URI) {
			return candidate
		}
	}
	return nil
}

func (a APIKeys) Len() int           { return len(a) }
func (a APIKeys) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a APIKeys) Less(i, j int) bool { return len(a[i].URI) > len(a[j].URI) }

func (a APIKeys) Init() error {
	sort.Sort(a)
	for _, item := range a {
		if err := item.Init(); err != nil {
			return err
		}
	}
	return nil
}
