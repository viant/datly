package provider

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly/locator"
	"github.com/viant/structology"
)

const constantKind = "const"

// Constants exposes immutable component constants as one named Bindly source.
func Constants(source map[string]string) (locator.Provider, error) {
	if len(source) == 0 {
		return nil, nil
	}
	values := make(map[string]string, len(source))
	for name, value := range source {
		canonical := strings.ToLower(strings.TrimSpace(name))
		if canonical == "" {
			return nil, fmt.Errorf("constant name is required")
		}
		if _, ok := values[canonical]; ok {
			return nil, fmt.Errorf("constant name %q is ambiguous", name)
		}
		values[canonical] = value
	}
	return &constantProvider{values: values}, nil
}

type constantProvider struct {
	values map[string]string
}

func (p *constantProvider) Kind() string           { return constantKind }
func (p *constantProvider) Priority() int          { return 0 }
func (p *constantProvider) DefaultCacheable() bool { return true }
func (p *constantProvider) Locate(*structology.State) locator.Locator {
	return &constantLocator{values: p.values}
}

// ComposeConstants combines same-kind named providers. The canonical provider
// is trusted only because runtime passes it through a separate engine request
// slot; callers cannot mark an ordinary provider as canonical.
func ComposeConstants(canonical locator.Provider, providers []locator.Provider) locator.Provider {
	if canonical == nil && len(providers) == 0 {
		return nil
	}
	if canonical == nil && len(providers) == 1 {
		return providers[0]
	}
	ordered := make([]locator.Provider, 0, len(providers)+1)
	if canonical != nil {
		ordered = append(ordered, canonical)
	}
	for index := len(providers) - 1; index >= 0; index-- {
		ordered = append(ordered, providers[index])
	}
	return &constantChainProvider{providers: ordered}
}

type constantChainProvider struct {
	providers []locator.Provider
}

func (p *constantChainProvider) Kind() string { return constantKind }

func (p *constantChainProvider) Priority() int {
	if len(p.providers) == 0 || p.providers[0] == nil {
		return 0
	}
	return p.providers[0].Priority()
}

func (p *constantChainProvider) DefaultCacheable() bool {
	if len(p.providers) == 0 || p.providers[0] == nil {
		return false
	}
	policy, ok := p.providers[0].(locator.CachePolicy)
	return ok && policy.DefaultCacheable()
}

func (p *constantChainProvider) Locate(state *structology.State) locator.Locator {
	locators := make([]locator.Locator, 0, len(p.providers))
	for index, candidate := range p.providers {
		if candidate == nil {
			locators = append(locators, &unavailableConstantLocator{index: index})
			continue
		}
		actual := candidate.Locate(state)
		if actual == nil {
			locators = append(locators, &unavailableConstantLocator{index: index})
			continue
		}
		locators = append(locators, actual)
	}
	return &constantChainLocator{locators: locators}
}

type constantChainLocator struct {
	locators []locator.Locator
}

func (l *constantChainLocator) Kind() string { return constantKind }

func (l *constantChainLocator) Value(ctx context.Context, rType reflect.Type, name string) (any, bool, error) {
	for _, candidate := range l.locators {
		value, found, err := candidate.Value(ctx, rType, name)
		if err != nil || found {
			return value, found, err
		}
	}
	return nil, false, nil
}

type unavailableConstantLocator struct {
	index int
}

func (l *unavailableConstantLocator) Kind() string { return constantKind }

func (l *unavailableConstantLocator) Value(context.Context, reflect.Type, string) (any, bool, error) {
	return nil, false, fmt.Errorf("const provider at precedence %d returned no locator", l.index)
}

type constantLocator struct {
	values map[string]string
}

func (l *constantLocator) Kind() string { return constantKind }

func (l *constantLocator) Value(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
	value, ok := l.values[strings.ToLower(strings.TrimSpace(name))]
	return value, ok, nil
}
