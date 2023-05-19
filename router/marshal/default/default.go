package _default

import (
	"github.com/viant/toolbox/format"
	"strings"
)

type Default struct {
	OmitEmpty  bool
	CaseFormat format.Case
	Exclude    map[string]bool
	DateLayout string
}

func NormalizeExclusionKey(item string) string {
	return strings.ToLower(strings.ReplaceAll(item, "_", ""))
}

type Exclude []string

func (e Exclude) Index() map[string]bool {
	var result = map[string]bool{}
	if len(e) == 0 {
		return result
	}
	for _, item := range e {
		result[item] = true
		result[NormalizeExclusionKey(item)] = true
	}
	return result
}
