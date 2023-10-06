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
	keys := s.Keys()
	for _, k := range keys {
		v := s[k]

		key := "${" + k + "}"
		if count := strings.Count(text, v); count > 0 {
			text = strings.Replace(text, v, key, count)
		}

	}
	return text
}
