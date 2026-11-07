package selectorcompanion

import (
	"context"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/spec"
)

// Root aliases reuse the native typed binding plan. They do not add canonical
// parameters or grant selector authority to child views or ordinary inputs.
type rootAliasPlan struct {
	primary, alias string
	plan           *bindly.Plan
}

func (p *BindingPlan) compileRootAliases(component *spec.Component, injector *bindly.Injector) error {
	if p.transportOnly || component.RootView == nil {
		return nil
	}
	for _, binding := range p.selectors {
		parameter := binding.Extension.(*spec.Parameter)
		selector := parameter.QuerySelector
		if binding.Location.Kind != "query" || selector == nil || binding.Required != nil && *binding.Required {
			continue
		}
		if !strings.EqualFold(selector.View, component.RootView.Name) && !strings.EqualFold(selector.View, component.RootView.CanonicalName()) {
			continue
		}
		if selector.Property != spec.SelectorPropertyFields && selector.Property != spec.SelectorPropertyOrderBy {
			continue
		}
		alias := "_" + strings.ToLower(string(selector.Property))
		if binding.Location.In == alias {
			continue
		}
		fallback := binding
		fallback.Location.In = alias
		plan, err := injector.CompilePlan(p.companion, fallback)
		if err != nil {
			return err
		}
		p.rootAliases = append(p.rootAliases, rootAliasPlan{primary: binding.Location.In, alias: alias, plan: plan})
	}
	return nil
}

func (p *BindingPlan) bindRootAliases(ctx context.Context, scope *bindly.Injector, input, target any, providers []locator.Provider) error {
	if len(p.rootAliases) == 0 {
		return nil
	}
	var query locator.Provider
	for _, provider := range providers {
		if provider == nil || provider.Kind() != "query" {
			continue
		}
		// Custom/composed providers retain their canonical behavior. Only an exact
		// invocation-owned native query source supplies historical HTTP aliases.
		if query != nil || reflect.TypeOf(provider) != nativeQueryProviderType || reflect.ValueOf(provider).IsNil() {
			return nil
		}
		query = provider
	}
	if query == nil {
		return nil
	}
	source := query.Locate(nil)
	if source == nil {
		return nil
	}
	for _, alias := range p.rootAliases {
		_, present, err := source.Value(ctx, nil, alias.primary)
		if err != nil {
			return err
		}
		if present {
			continue
		}
		_, present, err = source.Value(ctx, nil, alias.alias)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		if err = scope.Bind(ctx, target, bindly.WithPlan(alias.plan), bindly.WithSource(input)); err != nil {
			return err
		}
	}
	return nil
}
