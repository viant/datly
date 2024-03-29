package path

import (
	"context"
	"github.com/viant/scy"
	"sort"
	"strings"
)

type (
	APIKey struct {
		URI    string        `yaml:"URI,omitempty"`
		Value  string        `yaml:"GetValue,omitempty"`
		Header string        `yaml:"Header,omitempty"`
		Secret *scy.Resource `yaml:"Secret,omitempty"`
	}

	APIKeys []*APIKey
)

func (k *APIKey) Init(ctx context.Context) error {
	if k.Secret != nil {
		srv := scy.New()
		secret, err := srv.Load(ctx, k.Secret)
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

func (a APIKeys) Init(ctx context.Context) error {
	sort.Sort(a)
	for _, item := range a {
		if err := item.Init(ctx); err != nil {
			return err
		}
	}
	return nil
}
