package view

import "strings"

type Substitute struct {
	Key      string
	Fragment string
}

type Substitutes []*Substitute

func (s Substitutes) Replace(text string) string {
	if len(s) == 0 {
		return text
	}
	for _, item := range s {
		if count := strings.Count(text, item.Key); count > 0 {
			text = strings.Replace(text, item.Key, item.Fragment, count)
		}
	}
	return text
}

func (s Substitutes) ReverseReplace(text string) string {
	if len(s) == 0 {
		return text
	}
	for _, item := range s {
		if count := strings.Count(text, item.Fragment); count > 0 {
			text = strings.Replace(text, item.Fragment, item.Key, count)
		}
	}
	return text
}
