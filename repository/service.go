package repository

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/plugin"
	"github.com/viant/datly/repository/resource"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	"strings"
	"sync"
	"time"
)

type (
	Service struct {
		registry         *Registry
		paths            *path.Service
		resources        Resources
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

func (s *Service) Extensions() *extension.Registry {
	return s.extensions
}

func (s *Service) Resource() Resources {
	return s.resources
}

func (s *Service) Registry() *Registry {
	return s.registry
}

func (s *Service) Container() *path.Container {
	return &s.paths.Container
}

// SyncChanges checks if resource, plugin or components have changes
// if so it would increase individual or all component/paths version number resulting in lazy reload
func (s *Service) SyncChanges(ctx context.Context) (bool, error) {
	now := time.Now()
	//fmt.Printf("[INFO] sync changes started\n")
	snap := &snapshot{}

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

	//fmt.Printf("[INFO] sync changes completed after: %s\n", time.Since(now))
	return snap.majorChange, snap.error()
}

func (s *Service) init(ctx context.Context, options *Options) (err error) {
	if s.paths, err = path.New(ctx, options.fs, options.componentURL, options.refreshFrequency); err != nil {
		return err
	}
	if s.resources == nil && options.resourceURL != "" {
		if s.resources, err = resource.New(ctx, options.fs, options.resourceURL, options.refreshFrequency); err != nil {
			return err
		}
	}
	if s.resources == nil {
		s.resources, _ = resource.New(ctx, options.fs, "", options.refreshFrequency)
	}
	if !options.ignorePlugin {
		if s.plugins == nil && options.pluginURL != "" {
			if s.plugins, err = plugin.New(ctx, options.fs, options.pluginURL, options.refreshFrequency); err != nil {
				if !strings.Contains(err.Error(), " plugin already loaded") {
					fmt.Printf("WARNING: failed to load plugin: %v\n", err)
				}
			}
		}
	}
	if len(options.constants) > 0 {
		if constants, _ := s.resources.Lookup(view.ResourceConstants); constants != nil {
			for k, v := range options.constants {
				constants.Parameters = append(constants.Parameters, &state.Parameter{
					Name:  k,
					Value: v,
					In:    state.NewConstLocation(k),
				})
			}
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
				opts = append(opts, WithMetrics(s.options.metrics))
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
		WithPath(aPath),
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

// JWTSigner returns jwt signer
func (s *Service) JWTSigner() *signer.Service {
	return s.options.jwtSigner
}

// JWTVerifier returns jwt signer
func (s *Service) JWTVerifier() *verifier.Service {
	return s.options.jWTVerifier
}

func (s *Service) inheritFromPath(component *Component, aPath *path.Path) {
	component.dispatcher = s.registry.dispatcher
	if component.Output.RevealMetric == nil {
		component.Output.RevealMetric = aPath.RevealMetric
	}
}

func (s *Service) Constants() []*state.Parameter {
	res, err := s.resources.Lookup(view.ResourceConstants)
	if err != nil {
		return nil
	}
	return res.Parameters
}

func New(ctx context.Context, opts ...Option) (*Service, error) {
	options := NewOptions(opts)
	ret := &Service{
		options:          options,
		refreshFrequency: options.refreshFrequency,
		resources:        options.resources,
		extensions:       options.extensions,
		registry:         NewRegistry(options.apiPrefix, options.dispatcher),
	}
	err := ret.init(ctx, options)
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
