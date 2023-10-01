package repository

import (
	"context"
	"fmt"
	"github.com/viant/cloudless/gateway/matcher"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/repository/version"
	"sync"
)

type (
	Registry struct {
		apiPrefix string
		mux       sync.RWMutex
		index     map[string]*Provider
		entries
		matcher *matcher.Matcher
	}

	Provider struct {
		path          component.Path
		control       version.Control
		loadComponent func(ctx context.Context) (*Component, error)
		component     *Component
	}

	entries []*Provider
)

func (e entries) matchables() []matcher.Matchable {
	var result = make([]matcher.Matchable, 0, len(e))
	for _, item := range e {
		result = append(result, item)
	}
	return result
}

func indexKey(path *component.Path) string {
	return path.Method + ":" + path.URI
}

func (r *Provider) URI() string {
	return r.path.URI
}

func (r *Provider) Namespaces() []string {
	return []string{r.path.Method}
}

func (r *Registry) Lookup(path *component.Path) (*Component, error) {
	r.mux.RLock()
	defer r.mux.RUnlock()

	key := indexKey(path)
	ret, ok := r.index[key]
	if ok {
		return ret.component, nil
	}
	matchable, err := r.matcher.MatchOne(path.Method, path.URI)
	if err != nil {
		return nil, err
	}
	result, ok := matchable.(*Provider)
	if !ok {
		return nil, fmt.Errorf("expected: %T, but had: %T", result, matchable)
	}
	return result.component, nil
}

func (r *Registry) Register(components ...*Component) {
	r.mux.Lock()
	defer r.mux.Unlock()
	count := len(r.entries)
	for i := range components {
		aComponent := components[i]
		key := indexKey(&aComponent.Path)
		if prev, ok := r.index[key]; ok {
			prev.component = aComponent
			continue
		}
		anEntry := &Provider{
			path:      component.Path{Method: aComponent.Method, URI: aComponent.URI},
			component: aComponent,
		}
		r.index[key] = anEntry
		r.entries = append(r.entries, anEntry)
	}

	if count != len(r.entries) {
		r.matcher = matcher.NewMatcher(r.entries.matchables())
	}
}

func NewRegistry(apiPrefix string) *Registry {
	return &Registry{index: map[string]*Provider{}, apiPrefix: apiPrefix}
}
