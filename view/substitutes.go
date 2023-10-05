package view

import "strings"

type Substitutes map[string]string

func (s Substitutes) Replace(text string) string {
	if len(s) == 0 {
		return text
	}
	for k, v := range s {
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
	for k, v := range s {
		key := "${" + k + "}"
		if count := strings.Count(text, v); count > 0 {
			text = strings.Replace(text, v, key, count)
		}
	}
	return text
}
