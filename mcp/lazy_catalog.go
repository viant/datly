package mcp

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
)

type lazyCatalog struct {
	config     Config
	base       []*registry.RegisteredComponent
	targets    map[string]spec.Key
	components map[string]spec.Key
	prepared   map[string]*registry.RegisteredComponent
	active     atomic.Pointer[Service]
	mu         sync.Mutex
}

func newLazyCatalog(config Config, base *Service) (*lazyCatalog, error) {
	if config.Loader == nil {
		return nil, fmt.Errorf("indexed MCP component loader is required")
	}
	result := &lazyCatalog{config: config, base: append([]*registry.RegisteredComponent(nil), config.Components...), targets: map[string]spec.Key{}, components: map[string]spec.Key{}, prepared: map[string]*registry.RegisteredComponent{}}
	baseKeys := map[string]bool{}
	for _, component := range config.Components {
		if component != nil && component.Component != nil {
			baseKeys[component.Component.Key.String()] = true
		}
	}
	for _, component := range config.Indexed {
		if component == nil {
			return nil, fmt.Errorf("indexed MCP component metadata is required")
		}
		if baseKeys[component.Key.String()] {
			continue
		}
		for _, route := range component.Routes {
			if route == nil {
				return nil, fmt.Errorf("indexed MCP component %s has a nil route", component.Key.String())
			}
			for _, exposure := range route.MCP {
				if exposure == nil {
					return nil, fmt.Errorf("indexed MCP route has a nil exposure")
				}
				if exposure.Kind != spec.MCPExposureTool {
					return nil, fmt.Errorf("indexed MCP resources require explicit component materialization")
				}
				name := exposure.Identity(component, route)
				if name == "" || result.targets[name].Name != "" {
					return nil, fmt.Errorf("duplicate or empty indexed MCP tool name %q", name)
				}
				result.targets[name] = component.Key
				result.components[component.Key.String()] = component.Key
			}
		}
	}
	detached := *base
	detached.lazy = nil
	result.active.Store(&detached)
	return result, nil
}

func (l *lazyCatalog) service() *Service { return l.active.Load() }

func (l *lazyCatalog) prepareTool(ctx context.Context, name string) error {
	key, ok := l.targets[strings.TrimSpace(name)]
	if !ok {
		return nil
	}
	return l.prepare(ctx, []spec.Key{key})
}

func (l *lazyCatalog) prepareAll(ctx context.Context) error {
	keys := make([]spec.Key, 0, len(l.components))
	for _, key := range l.components {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return l.prepare(ctx, keys)
}

func (l *lazyCatalog) prepare(ctx context.Context, keys []spec.Key) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	changed := false
	for _, key := range keys {
		identity := key.String()
		if l.prepared[identity] != nil {
			continue
		}
		loaded, err := l.config.Loader.LoadComponent(ctx, key)
		if err != nil {
			return err
		}
		l.prepared[identity] = loaded
		if family, ok := l.config.Loader.(interface {
			LoadComponents(context.Context, spec.Key) ([]*registry.RegisteredComponent, error)
		}); ok {
			registrations, familyErr := family.LoadComponents(ctx, key)
			if familyErr != nil {
				return familyErr
			}
			for _, related := range registrations {
				if related != nil && related.Component != nil {
					l.prepared[related.Component.Key.String()] = related
				}
			}
		}
		changed = true
	}
	if !changed {
		return nil
	}
	components := append([]*registry.RegisteredComponent(nil), l.base...)
	identities := make([]string, 0, len(l.prepared))
	for identity := range l.prepared {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	for _, identity := range identities {
		components = append(components, l.prepared[identity])
	}
	config := l.config
	config.Components = components
	config.Indexed = nil
	config.Loader = nil
	if config.Authorization != nil && len(config.Authorization.Tools) > 0 {
		policy := cloneAuthorizationPolicy(config.Authorization)
		policy.Tools = map[string]*authorization.Authorization{}
		for name, key := range l.targets {
			if rule, exists := config.Authorization.Tools[name]; exists && l.prepared[key.String()] != nil {
				policy.Tools[name] = cloneAuthorization(rule)
			}
		}
		config.Authorization = policy
	}
	compiled, err := (&serviceCompiler{config: config}).Compile(ctx)
	if err != nil {
		return err
	}
	l.active.Store(compiled)
	return nil
}

func compileIndexedAuthorizationPolicy(source *authorization.Policy, catalog *Catalog, indexed []*spec.Component) (*authorization.Policy, error) {
	policy := cloneAuthorizationPolicy(source)
	if policy == nil {
		return nil, nil
	}
	if policy.Global != nil && len(policy.Tools) > 0 {
		return nil, fmt.Errorf("MCP global authorization cannot be combined with tool or resource rules")
	}
	names := map[string]bool{}
	for _, component := range indexed {
		if component == nil {
			continue
		}
		for _, route := range component.Routes {
			for _, exposure := range route.MCP {
				if exposure != nil && exposure.Kind == spec.MCPExposureTool {
					names[exposure.Identity(component, route)] = true
				}
			}
		}
	}
	for name := range policy.Tools {
		if _, ok := catalog.Tool(name); !ok && !names[name] {
			return nil, fmt.Errorf("MCP authorization references unknown tool %q", name)
		}
		if err := validateAuthorizationRule("tool "+name, policy.Tools[name]); err != nil {
			return nil, err
		}
	}
	withoutTools := cloneAuthorizationPolicy(policy)
	withoutTools.Tools = nil
	if _, err := compileAuthorizationPolicy(withoutTools, catalog); err != nil {
		return nil, err
	}
	return policy, nil
}
