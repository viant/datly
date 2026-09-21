package spec

import (
	"fmt"
	"strings"
)

func (s *CacheWarmupSettings) Clone() *CacheWarmupSettings {
	if s == nil {
		return nil
	}
	cloned := *s
	cloned.FieldNames = append([]string(nil), s.FieldNames...)
	cloned.CaseRefs = append([]string(nil), s.CaseRefs...)
	if s.Limit != nil {
		value := *s.Limit
		cloned.Limit = &value
	}
	if s.MaxCases != nil {
		value := *s.MaxCases
		cloned.MaxCases = &value
	}
	if s.Cases != nil {
		cloned.Cases = make([]*CacheWarmupCase, len(s.Cases))
		for i, item := range s.Cases {
			if item != nil {
				cloned.Cases[i] = item.Clone()
			}
		}
	}
	return &cloned
}

func (c *CacheWarmupCase) Clone() *CacheWarmupCase {
	if c == nil {
		return nil
	}
	cloned := *c
	cloned.FieldNames = append([]string(nil), c.FieldNames...)
	if c.Set != nil {
		cloned.Set = make([]*CacheWarmupParam, len(c.Set))
		for i, item := range c.Set {
			if item != nil {
				cloned.Set[i] = item.Clone()
			}
		}
	}
	return &cloned
}

func (p *CacheWarmupParam) Clone() *CacheWarmupParam {
	if p == nil {
		return nil
	}
	cloned := *p
	cloned.Values = append([]string(nil), p.Values...)
	return &cloned
}

// EffectiveName canonicalizes an absent Name from IndexParameter, falling back to IndexColumn.
func (s *CacheWarmupSettings) EffectiveName() string {
	if s == nil {
		return ""
	}
	if name := strings.TrimSpace(s.Name); name != "" {
		return name
	}
	if name := strings.TrimSpace(s.IndexParameter); name != "" {
		return name
	}
	return strings.TrimSpace(s.IndexColumn)
}

// CloneSharedCases deep-copies named reusable case sets.
func CloneSharedCases(caseSets map[string][]*CacheWarmupCase) map[string][]*CacheWarmupCase {
	if caseSets == nil {
		return nil
	}
	cloned := make(map[string][]*CacheWarmupCase, len(caseSets))
	for name, cases := range caseSets {
		items := make([]*CacheWarmupCase, 0, len(cases))
		for _, item := range cases {
			items = append(items, item.Clone())
		}
		cloned[name] = items
	}
	return cloned
}

// Clone returns detached cache settings including every warmup definition.
func (c *CacheSettings) Clone() *CacheSettings {
	if c == nil {
		return nil
	}
	cloned := *c
	cloned.Warmup = c.Warmup.Clone()
	if c.Warmups != nil {
		cloned.Warmups = make([]*CacheWarmupSettings, len(c.Warmups))
		for i, item := range c.Warmups {
			cloned.Warmups[i] = item.Clone()
		}
	}
	cloned.SharedCases = CloneSharedCases(c.SharedCases)
	return &cloned
}

// HasWarmup reports whether the settings define a singular or plural warmup.
func (c *CacheSettings) HasWarmup() bool {
	if c == nil {
		return false
	}
	if c.Warmup != nil {
		return true
	}
	for _, item := range c.Warmups {
		if item != nil {
			return true
		}
	}
	return false
}

// EffectiveWarmups returns every normalized warmup definition: singular first,
// then plural in declaration order, with CaseRefs expanded from SharedCases.
func (c *CacheSettings) EffectiveWarmups() ([]*CacheWarmupSettings, error) {
	if c == nil {
		return nil, nil
	}
	return EffectiveCacheWarmups(c.Warmup, c.Warmups, c.SharedCases)
}

// EffectiveCacheWarmups normalizes a singular-plus-plural warmup declaration to
// an ordered, detached warmup list. Each returned warmup owns an immutable copy
// of its cases: referenced case sets expand before inline cases, duplicates
// within one warmup are removed by canonical value identity, and no case slice
// is shared between warmups. Duplicate effective names or index identities are
// rejected instead of overwriting.
func EffectiveCacheWarmups(warmup *CacheWarmupSettings, warmups []*CacheWarmupSettings, shared map[string][]*CacheWarmupCase) ([]*CacheWarmupSettings, error) {
	var ordered []*CacheWarmupSettings
	if warmup != nil {
		ordered = append(ordered, warmup)
	}
	for _, item := range warmups {
		if item != nil {
			ordered = append(ordered, item)
		}
	}
	if len(ordered) == 0 {
		return nil, nil
	}
	result := make([]*CacheWarmupSettings, 0, len(ordered))
	seenNames := make(map[string]bool, len(ordered))
	seenIndexes := make(map[string]bool, len(ordered))
	for _, item := range ordered {
		expanded, err := expandWarmupCaseRefs(item, shared)
		if err != nil {
			return nil, err
		}
		if len(ordered) > 1 {
			name := strings.ToLower(expanded.EffectiveName())
			if seenNames[name] {
				return nil, fmt.Errorf("duplicate warmup name %q", name)
			}
			seenNames[name] = true
			indexIdentity := strings.ToLower(strings.Join([]string{strings.TrimSpace(item.IndexColumn), strings.TrimSpace(item.IndexParameter)}, "|"))
			if seenIndexes[indexIdentity] {
				return nil, fmt.Errorf("duplicate warmup index identity %q (indexColumn|indexParameter)", indexIdentity)
			}
			seenIndexes[indexIdentity] = true
		}
		result = append(result, expanded)
	}
	return result, nil
}

// expandWarmupCaseRefs returns a detached warmup whose referenced case sets are
// expanded ahead of inline cases with canonical per-warmup deduplication.
func expandWarmupCaseRefs(warmup *CacheWarmupSettings, shared map[string][]*CacheWarmupCase) (*CacheWarmupSettings, error) {
	cloned := warmup.Clone()
	if len(cloned.CaseRefs) == 0 {
		return cloned, nil
	}
	var expanded []*CacheWarmupCase
	for _, ref := range cloned.CaseRefs {
		ref = strings.TrimSpace(ref)
		cases, ok := shared[ref]
		if !ok {
			return nil, fmt.Errorf("not found warmup case set %q referenced by warmup %v", ref, cloned.EffectiveName())
		}
		for _, item := range cases {
			expanded = append(expanded, item.Clone())
		}
	}
	cloned.Cases = dedupeWarmupCases(append(expanded, cloned.Cases...))
	return cloned, nil
}

// dedupeWarmupCases removes duplicated cases within one warmup by canonical
// parameter name and value; it also makes repeated CaseRefs expansion idempotent.
func dedupeWarmupCases(cases []*CacheWarmupCase) []*CacheWarmupCase {
	result := make([]*CacheWarmupCase, 0, len(cases))
	seen := make(map[string]bool, len(cases))
	for _, item := range cases {
		key := warmupCaseKey(item)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, item)
	}
	return result
}

func warmupCaseKey(item *CacheWarmupCase) string {
	if item == nil {
		return ""
	}
	builder := strings.Builder{}
	for _, param := range item.Set {
		if param == nil {
			continue
		}
		builder.WriteString(strings.TrimSpace(param.Name))
		builder.WriteString("=")
		builder.WriteString(strings.Join(param.Values, "\x00"))
		builder.WriteString(fmt.Sprintf(";excludeDefault=%v|", param.ExcludeDefault))
	}
	builder.WriteString("fields=")
	builder.WriteString(strings.Join(item.FieldNames, ","))
	return builder.String()
}
