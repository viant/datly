package repository

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/plugin"
	"github.com/viant/datly/repository/resource"
	"github.com/viant/datly/view/extension"
	"strings"
	"sync"
	"time"
)

type (
	Service struct {
		registry         *Registry
		paths            *path.Service
		resources        *resource.Service
		extensions       *extension.Registry
		plugins          *plugin.Service
		refreshFrequency time.Duration
		options          *Options
	}

	snapshot struct {
		mux         sync.Mutex
		group       sync.WaitGroup
		errors      []error
		majorChange bool
	}
)

func (s *Service) Registry() *Registry {
	return s.registry
}

func (s *Service) Container() *path.Container {
	return &s.paths.Container
}

// SyncChanges checks if resource, plugin or components have changes
// if so it would increase individual or all component/paths version number resulting in lazy reload
func (s *Service) SyncChanges(ctx context.Context) (bool, error) {
	snap := &snapshot{}
	now := time.Now()
	if s.plugins.IsCheckDue(now) {
		snap.group.Add(1)
		go func() {
			defer snap.group.Done()
			snap.addStatus(s.plugins.SyncChanges(ctx))
		}()
	}
	if s.resources.IsCheckDue(now) {
		snap.group.Add(1)
		go func() {
			defer snap.group.Done()
			snap.addStatus(s.resources.SyncChanges(ctx))
		}()
	}
	if s.paths.IsCheckDue(now) {
		snap.group.Add(1)
		go func() {
			defer snap.group.Done()
			routesLen := len(s.paths.Container.Items)
			_, err := s.paths.SyncChanges(ctx) //modification changes should not trigger reset,
			// but adding or removing should
			snap.addStatus(routesLen != len(s.paths.Container.Items), err)
		}()
	}
	snap.group.Wait()
	if snap.majorChange {
		s.paths.IncreaseVersion()
	}
	return snap.majorChange, snap.error()
}

func (s *Service) init(ctx context.Context, URL string, options *Options) (err error) {
	if s.paths, err = path.New(ctx, options.fs, URL, options.refreshFrequency); err != nil {
		return err
	}
	if s.resources == nil && options.resourceURL != "" {
		if s.resources, err = resource.New(ctx, options.fs, options.resourceURL, options.refreshFrequency); err != nil {
			return err
		}
	}
	if s.plugins == nil && options.pluginURL != "" {
		if s.plugins, err = plugin.New(ctx, options.fs, options.pluginURL, options.refreshFrequency); err != nil {
			fmt.Printf("ERROR: failed to load plugin: %v\n", err)
		}
	}
	return s.initProviders(ctx)
}

func (s *Service) initProviders(ctx context.Context) error {
	paths := s.paths.GetPaths()
	pathsLen := len(paths.Items)
	var providers []*Provider
	for i := 0; i < pathsLen; i++ {
		route := paths.Items[i]
		sourceURL := route.SourceURL
		if url.IsRelative(sourceURL) {
			sourceURL = url.Join(s.paths.URL, sourceURL)
		}
		for _, aPath := range route.Paths {
			provider := NewProvider(aPath.Path, aPath.Version, func(ctx context.Context, opts ...Option) (*Component, error) {
				component, err := s.loadComponent(ctx, opts, sourceURL, aPath)
				if err != nil || component != nil {
					return component, err
				}
				return nil, fmt.Errorf("no component for path: %s", aPath.Path.Key())
			})
			providers = append(providers, provider)
		}
	}
	s.registry.SetProviders(providers)
	return nil
}

func (s *Service) loadComponent(ctx context.Context, opts []Option, sourceURL string, aPath *path.Path) (*Component, error) {
	opts = append([]Option{
		WithResources(s.resources),
		WithExtensions(s.extensions),
	}, opts...)
	components, err := LoadComponents(ctx, sourceURL, opts...)
	if err != nil {
		return nil, err
	}
	if err = components.Init(ctx); err != nil {
		return nil, err
	}
	for _, component := range components.Components {
		s.inheritFromPath(component, aPath)
	}
	for _, candidate := range components.Components {
		if candidate.Path.Equals(&aPath.Path) {
			if err = s.updateCacheConnectorRef(components.Resource, candidate.View); err != nil {
				return nil, err
			}
			return candidate, nil
		}
	}
	return nil, nil
}

func (s *Service) inheritFromPath(component *Component, aPath *path.Path) {
	component.dispatcher = s.registry.dispatcher
	if component.Output.RevealMetric == nil {
		component.Output.RevealMetric = aPath.RevealMetric
	}
}

func New(ctx context.Context, componentsURL string, opts ...Option) (*Service, error) {
	options := NewOptions(componentsURL, opts...)
	ret := &Service{
		options:          options,
		refreshFrequency: options.refreshFrequency,
		resources:        options.resources,
		extensions:       options.extensions,
		registry:         NewRegistry(options.apiPrefix, options.dispatcher),
	}
	err := ret.init(ctx, componentsURL, options)
	return ret, err
}

func (s *snapshot) addStatus(changed bool, err error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if err != nil {
		s.errors = append(s.errors, err)
	}
	if changed {
		s.majorChange = changed
	}
}

func (s *snapshot) error() error {
	switch len(s.errors) {
	case 0:
		return nil
	case 1:
		return s.errors[0]
	default:
		var errors = strings.Builder{}
		for i, err := range s.errors {
			if i > 0 {
				errors.WriteString(",")
			}
			errors.WriteString(err.Error())
		}
		return fmt.Errorf(errors.String()+"; %w", s.errors[0])
	}
}
