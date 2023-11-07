package repository

import (
	"context"
	"fmt"
	"github.com/viant/cloudless/gateway/matcher"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
	"sync"
)

type (
	Registry struct {
		apiPrefix string
		mux       sync.RWMutex
		index     map[string]*Provider
		providers
		matcher    *matcher.Matcher
		dispatcher contract.Dispatcher
	}

	providers []*Provider
)

func (e providers) matchables() []matcher.Matchable {
	var result = make([]matcher.Matchable, 0, len(e))
	for _, item := range e {
		result = append(result, item)
	}
	return result
}

func indexKey(path *contract.Path) string {
	return path.Method + ":" + path.URI
}

func (r *Provider) URI() string {
	return r.path.URI
}

func (r *Provider) Namespaces() []string {
	return []string{r.path.Method}
}

func (r *Registry) LookupProvider(ctx context.Context, path *contract.Path) (*Provider, error) {
	r.mux.RLock()
	defer r.mux.RUnlock()
	key := indexKey(path)
	ret, ok := r.index[key]
	if ok {
		return ret, nil
	}
	matchable, err := r.matcher.MatchOne(path.Method, path.URI)
	if err != nil {
		return nil, err
	}
	result, ok := matchable.(*Provider)
	if !ok {
		return nil, fmt.Errorf("expected: %T, but had: %T", result, matchable)
	}
	return result, nil
}

func (r *Registry) Lookup(ctx context.Context, path *contract.Path, opts ...Option) (*Component, error) {
	provider, err := r.LookupProvider(ctx, path)
	if err != nil {
		return nil, err
	}
	return provider.Component(ctx, opts...)
}

func (r *Registry) Register(components ...*Component) {
	r.mux.Lock()
	defer r.mux.Unlock()
	count := len(r.providers)
	for i := range components {
		aComponent := components[i]
		key := indexKey(&aComponent.Path)
		if prev, ok := r.index[key]; ok {
			prev.component = aComponent
			continue
		}
		if aComponent.dispatcher == nil {
			aComponent.dispatcher = r.dispatcher
		}
		aProvider := &Provider{
			path:      contract.Path{Method: aComponent.Method, URI: aComponent.URI},
			component: aComponent,
			control:   &version.Control{},
		}
		r.index[key] = aProvider
		r.providers = append(r.providers, aProvider)
	}

	if count != len(r.providers) {
		r.matcher = matcher.NewMatcher(r.providers.matchables())
	}
}

func (r *Registry) SetProviders(providers []*Provider) {
	r.providers = providers
	r.index = map[string]*Provider{}
	for _, provider := range r.providers {
		r.index[provider.path.Key()] = provider
	}
	r.matcher = matcher.NewMatcher(r.providers.matchables())
}

func (r *Registry) SetComponents(components []*Component) {
	r.index = map[string]*Provider{}
	r.providers = nil
	r.Register(components...)
}

func (r *Registry) Dispatcher() contract.Dispatcher {
	return r.dispatcher
}

func NewRegistry(apiPrefix string, newDispatcher func(registry *Registry) contract.Dispatcher) *Registry {
	ret := &Registry{index: map[string]*Provider{}, apiPrefix: apiPrefix}

	if newDispatcher != nil {
		ret.dispatcher = newDispatcher(ret)
	}
	return ret
}
