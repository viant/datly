package view

import (
	"sort"
	"strings"
)

type Substitutes map[string]string

func (s Substitutes) Keys() []string {
	var result []string
	for k := range s {
		result = append(result, k)
	}
	sort.Slice(result, func(i, j int) bool {
		return len(result[i]) > len(result[j])
	})
	return result
}
func (s Substitutes) Replace(text string) string {
	if len(s) == 0 {
		return text
	}

	keys := s.Keys()
	for _, k := range keys {
		v := s[k]
		key := "${" + k + "}"
		if count := strings.Count(text, key); count > 0 {
			text = strings.Replace(text, key, v, count)
		}
		key = "$" + k
		if count := strings.Count(text, key); count > 0 {
			text = strings.Replace(text, key, v, count)
		}
	}
	return text
}

func (s Substitutes) ReverseReplace(text string) string {
	if len(s) == 0 {
		return text
	}
	// Build pairs of (key, value)
	pairs := make([]struct{ k, v string }, 0, len(s))
	for k, v := range s {
		pairs = append(pairs, struct{ k, v string }{k, v})
	}
	// Sort by value length desc, tie-breaker by key asc for stability
	sort.SliceStable(pairs, func(i, j int) bool {
		if len(pairs[i].v) == len(pairs[j].v) {
			return pairs[i].k < pairs[j].k
		}
		return len(pairs[i].v) > len(pairs[j].v)
	})
	// Replace using value-first order
	for _, p := range pairs {
		if p.v == "" {
			continue
		}
		key := "${" + p.k + "}"
		text = strings.ReplaceAll(text, p.v, key)
	}
	return text
}
