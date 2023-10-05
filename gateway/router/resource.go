package router

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	extension2 "github.com/viant/datly/view/extension"
	"github.com/viant/xdatly/codec"
	"time"
)

type (
	Resource struct {
		URL         string   `json:",omitempty" yaml:",omitempty"`
		SourceURL   string   `json:",omitempty"`
		With        []string //list of resource to inherit from  `json:",omitempty"`
		Routes      Routes
		Compression *path.Compression `json:",omitempty"`
		Redirect    *path.Redirect    `json:",omitempty"`
		Logger      *path.Logger      `json:",omitempty"` //connect, dataview, time, SQL with params if exceeded time  `json:",omitempty"`
		Cors        *path.Cors        `json:",omitempty"`

		ColumnsCache *discover.Columns `json:",omitempty"`
		RevealMetric *bool             `json:",omitempty"`

		ColumnsDiscovery bool  `json:",omitempty"`
		EnableDebug      *bool `json:",omitempty"`

		_codecs      *codec.Registry
		_initialised bool

		fs       afs.Service
		Resource *view.Resource
	}

	Logger struct {
		MinExecutionMs *int
	}
)

func (r *Resource) Init(ctx context.Context) error {
	if r._initialised {
		return nil
	}
	transforms := marshal.TransformIndex{}
	for _, route := range r.Routes {
		if err := route.normalizePaths(); err != nil {
			return err
		}
		if route.View.Ref == "" {
			continue
		}
		route.Transforms, transforms[route.View.Ref] = r.filterTransforms(route)
	}
	r._initialised = true
	var columnCacheExists bool
	if r.ColumnsDiscovery {
		parent, name := url.Split(r.SourceURL, file.Scheme)
		metaURL := url.Join(parent, ".meta", name)
		r.ColumnsCache = discover.New(metaURL, r.fs)
		if columnCacheExists = r.ColumnsCache.Exists(ctx); columnCacheExists {
			if err := r.ColumnsCache.Load(ctx); err != nil {
				return err
			}
			r.ColumnsCache.SourceURL = metaURL
		}
	}

	for _, route := range r.Routes {
		route._resource = r.Resource
		if route.Output.RevealMetric == nil {
			route.Output.RevealMetric = r.RevealMetric
		}
		route.EnableDebug = r.EnableDebug
	}

	columnsCache := map[string]view.Columns{}
	if r.ColumnsDiscovery {
		columnsCache = r.ColumnsCache.Items
	}

	if err := r.Resource.Init(ctx, r.Resource.TypeRegistry(), r._codecs, columnsCache, transforms, extension2.Config.Predicates); err != nil {
		return err
	}

	for _, route := range r.Routes {
		if err := route.Init(ctx, r); err != nil {
			return err
		}
	}

	if err := r.addLoggersIfNeeded(); err != nil {
		return err
	}

	if r.ColumnsDiscovery && (!columnCacheExists || r.Resource.ModTime.After(r.ColumnsCache.ModTime)) {
		r.ColumnsCache.ModTime = r.Resource.ModTime
		if err := r.ColumnsCache.Store(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (r *Resource) filterTransforms(route *Route) (routeTransforms marshal.Transforms, viewTransforms marshal.Transforms) {
	for _, transform := range route.Transforms {
		if transform.ParamName == "" {
			viewTransforms = append(viewTransforms, transform)
		} else {
			routeTransforms = append(routeTransforms, transform)
		}
	}

	return routeTransforms, viewTransforms
}

func (r *Resource) addLoggersIfNeeded() error {
	if r.Logger == nil {
		return nil
	}

	if r.Logger.MinExecutionMs == nil {
		return fmt.Errorf("unspecified logger MinExecutionMs")
	}

	duration := time.Millisecond * (time.Duration(*r.Logger.MinExecutionMs))
	timeLogger := logger.NewLogger("TimeLogger", logger.NewTimeLogger(duration, duration))

	for _, aRoute := range r.Routes {
		r.addLogger(aRoute.View, timeLogger)
	}

	return nil
}

func (r *Resource) addLogger(aView *view.View, timeLogger *logger.Adapter) {
	if aView.Logger != nil {
		aView.Logger = timeLogger
	}
	for _, relation := range aView.With {
		r.addLogger(&relation.Of.View, timeLogger)
	}
}
