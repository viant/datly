package data

import (
	"datly/base"
	"github.com/viant/toolbox"
	"strings"
)

//Selector represent a data selector for projection and selection
type Selector struct {
	Prefix   string `json:",omitempty"`
	Columns  []string `json:",omitempty"`
	Criteria *Criteria `json:",omitempty"`
	OrderBy  string `json:",omitempty"`
	Limit    int `json:",omitempty"`
	Offset   int `json:",omitempty"`
}

//Clone clones this selector
func (s Selector) Clone() *Selector {
	return &Selector{
		Prefix:   s.Prefix,
		Columns:  s.Columns,
		Criteria: s.Criteria,
		Limit:    s.Limit,
		Offset:   s.Offset,
	}
}

func (s *Selector) Apply(bindings map[string]interface{}) {

	if value, ok := bindings[s.Prefix+base.FieldsKey]; ok {
		if fields := toolbox.AsString(value); value != "" {
			s.Columns = asStringSlice(fields)
		}
	}
	if value, ok := bindings[s.Prefix+base.OrderByKey]; ok {
		s.OrderBy = toolbox.AsString(value)
	}

	if value, ok := bindings[s.Prefix+base.CriteriaKey]; ok {
		if s.Criteria == nil {
			s.Criteria = &Criteria{}
		}
		s.Criteria.Expression = toolbox.AsString(value)
		if value, ok := bindings[s.Prefix+base.ParamsKey]; ok {
			if fields := toolbox.AsString(value); value != "" {
				s.Criteria.Params = asStringSlice(fields)
			}
		}
	}
	if value, ok := bindings[s.Prefix+base.LimitKey]; ok {
		s.Limit = toolbox.AsInt(value)
	}
	if value, ok := bindings[s.Prefix+base.OffsetKey]; ok {
		s.Offset = toolbox.AsInt(value)
	}
}

func asStringSlice(text string) []string {
	var result = make([]string, 0)
	items := strings.Split(text, ",")
	for _, item := range items {
		result = append(result, strings.TrimSpace(item))
	}
	return result
}
