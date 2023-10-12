package config

import (
	"github.com/viant/structology/format/text"
	"strings"
)

type IOConfig struct {
	OmitEmpty  bool
	CaseFormat text.CaseFormat
	Exclude    map[string]bool
	DateFormat string
	TimeLayout string
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
