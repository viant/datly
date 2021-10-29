package router

import "strings"

type terminators []string

var _terminators = terminators{"{", "?", ";", "."}

func (t terminators) Index(text string) int {
	for _, candidate := range t {
		index := strings.Index(text, candidate)
		if index != -1 {
			return index
		}
	}
	return len(text)
}
