package config

import (
	"github.com/viant/tagly/format/text"
	ftime "github.com/viant/tagly/format/time"
	"strings"
)

type IOConfig struct {
	OmitEmpty  bool
	CaseFormat text.CaseFormat
	Exclude    map[string]bool
	DateFormat string
	TimeLayout string
}

func (c *IOConfig) GetTimeLayout() string {
	if c.TimeLayout != "" {
		return c.TimeLayout
	}
	if c.DateFormat == "" {
		return ""
	}
	c.TimeLayout = ftime.DateFormatToTimeLayout(c.DateFormat)
	return c.TimeLayout
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
