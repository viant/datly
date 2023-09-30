package gateway

import (
	"context"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/repository/component"
	"sync"
)

type (
	RouterLoader struct {
		sync.Once
		URI    string
		loader NewRouterFn
		router *router.Router
		err    error
	}

	LazyRouteContract struct {
		component.Path
		Cors   bool   `json:",omitempty"`
		Warmup bool   `json:",omitempty"`
		Name   string `json:",omitempty"`
	}

	LazyRouterContract struct {
		RouterURI string
		Routes    []*LazyRouteContract
	}

	RouteLoader struct {
		sync.Once
		LazyRouteContract
		routerLoader *RouterLoader
		route        *router.Route
		err          error
	}

	LazyRoute struct {
		sync.Once
		RouteMeta

		loader    *RouteLoader
		newRoute  NewRouteFn
		keyFinder ApiKeyFinderFn
		route     *Route
		err       error
	}

	NewRouteFn     func(ctx context.Context, router *router.Router, route *router.Route) (*Route, error)
	NewRouterFn    func(ctx context.Context, URI string) (*router.Router, error)
	ApiKeyFinderFn func(URI string) []*ApiKeyWrapper
)

func (l *LazyRoute) URI() string {
	return l.RouteMeta.URL
}

func (l *LazyRoute) Namespaces() []string {
	return []string{l.RouteMeta.Method}
}

func NewRouterLoader(URI string, loader NewRouterFn) *RouterLoader {
	return &RouterLoader{
		loader: loader,
		URI:    URI,
	}
}

func NewRouteLoader(contract LazyRouteContract, loader *RouterLoader) *RouteLoader {
	return &RouteLoader{
		LazyRouteContract: contract,
		routerLoader:      loader,
	}
}

func NewLazyRoute(path RouteMeta, loader *RouteLoader, fn NewRouteFn, keyFinder ApiKeyFinderFn) *LazyRoute {
	return &LazyRoute{
		RouteMeta: path,
		loader:    loader,
		newRoute:  fn,
		keyFinder: keyFinder,
	}
}

func (l *LazyRoute) Route(ctx context.Context) (*Route, error) {
	l.Do(func() {
		l.route, l.err = l.init(ctx)
	})

	return l.route, l.err
}

func (l *LazyRoute) init(ctx context.Context) (*Route, error) {
	aRouter, err := l.loader.Router(ctx)
	if err != nil {
		return nil, err
	}

	route, err := l.loader.Route(ctx)
	if err != nil {
		return nil, err
	}

	aRoute, err := l.newRoute(ctx, aRouter, route)
	if err != nil {
		return nil, err
	}

	keys := l.keyFinder(aRoute.URL)

	for _, key := range keys {
		aRoute.ApiKeys = append(aRoute.ApiKeys, key.apiKey)
	}

	return aRoute, err
}

func (r *RouterLoader) Load(ctx context.Context) (*router.Router, error) {
	r.Do(func() {
		r.router, r.err = r.loader(ctx, r.URI)
	})

	return r.router, r.err
}

func (l *RouteLoader) Route(ctx context.Context) (*router.Route, error) {
	l.Do(func() {
		l.route, l.err = l.init(ctx)
	})

	return l.route, l.err
}

func (l *RouteLoader) init(ctx context.Context) (*router.Route, error) {
	aRouter, err := l.Router(ctx)
	if err != nil {
		return nil, err
	}

	if l.Name != "" {
		return aRouter.MatchByName(l.Name)
	}

	return aRouter.MatchRoute(l.Path.Method, l.Path.URI)
}

func (l *RouteLoader) Router(ctx context.Context) (*router.Router, error) {
	aRouter, err := l.routerLoader.Load(ctx)
	if err != nil {
		return nil, err
	}
	return aRouter, nil
}
