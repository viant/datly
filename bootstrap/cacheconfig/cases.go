package cacheconfig

import (
	"fmt"
	"github.com/viant/datly/spec"
	"strings"
)

// Cases expands authored parameter alternatives without binding or converting
// values. Canonical Bindly source providers retain those responsibilities.
type Cases struct {
	Settings  *spec.CacheWarmupSettings
	Required  map[string]bool
	EntryCost int
}

type Case struct {
	Values     map[string]any
	FieldNames []string
}

func (c Cases) ForEach(visit func(Case) error) error {
	if c.Settings == nil || visit == nil {
		return fmt.Errorf("warmup settings and case visitor are required")
	}
	if c.Settings.MaxCases != nil && *c.Settings.MaxCases < 0 || c.Settings.Limit != nil && *c.Settings.Limit < 0 {
		return fmt.Errorf("warmup limits must be non-negative")
	}
	sets := c.Settings.Cases
	if len(sets) == 0 {
		sets = []*spec.CacheWarmupCase{{}}
	}
	for _, set := range sets {
		if set == nil {
			return fmt.Errorf("warmup case is nil")
		}
		seen := map[string]bool{}
		for _, parameter := range set.Set {
			if parameter == nil || strings.TrimSpace(parameter.Name) == "" {
				return fmt.Errorf("warmup parameter name is required")
			}
			if seen[parameter.Name] {
				return fmt.Errorf("duplicate warmup parameter %q", parameter.Name)
			}
			seen[parameter.Name] = true
			if len(parameter.Values) == 0 && (c.Required[parameter.Name] || parameter.ExcludeDefault) {
				return fmt.Errorf("warmup parameter %q has no values", parameter.Name)
			}
		}
	}
	count := 0
	cost := c.EntryCost
	if cost <= 0 {
		cost = 1
	}
	for _, set := range sets {
		fields := set.FieldNames
		if len(fields) == 0 {
			fields = c.Settings.FieldNames
		}
		values := map[string]any{}
		var expand func(int) error
		expand = func(index int) error {
			if c.Settings.MaxCases != nil && *c.Settings.MaxCases > 0 && cost > *c.Settings.MaxCases-count {
				return nil
			}
			if index == len(set.Set) {
				result := Case{Values: make(map[string]any, len(values)), FieldNames: append([]string(nil), fields...)}
				for name, value := range values {
					result.Values[name] = value
				}
				count += cost
				return visit(result)
			}
			parameter := set.Set[index]
			for _, value := range parameter.Values {
				values[parameter.Name] = value
				if err := expand(index + 1); err != nil {
					return err
				}
			}
			if !parameter.ExcludeDefault && !c.Required[parameter.Name] {
				values[parameter.Name] = nil
				if err := expand(index + 1); err != nil {
					return err
				}
			}
			delete(values, parameter.Name)
			return nil
		}
		if err := expand(0); err != nil {
			return err
		}
	}
	return nil
}
