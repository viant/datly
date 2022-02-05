package data

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox"
	"strings"
)

//type ClientSelector struct {
//	Columns    []string `json:",omitempty"`
//	Prefix     string   `json:",omitempty"`
//	OrderBy    string   `json:",omitempty"`
//	Offset     int      `json:",omitempty"`
//	CaseFormat string   `json:",omitempty"`
//	Limit      int      `json:",omitempty"`
//	OmitEmpty  bool     `json:",omitempty"`
//}

//Selector represent a data selector for projection and selection
type Selector struct {
	Columns         []string       `json:",omitempty"`
	Prefix          string         `json:",omitempty"`
	OrderBy         string         `json:",omitempty"`
	Offset          int            `json:",omitempty"`
	CaseFormat      string         `json:",omitempty"`
	Limit           int            `json:",omitempty"`
	OmitEmpty       bool           `json:",omitempty"`
	ExcludedColumns []string       `json:",omitempty"`
	Criteria        *data.Criteria `json:",omitempty"`
	selected        map[string]bool
	excludedColumns map[string]bool
}

//Clone clones this selector
func (s Selector) Clone() *Selector {
	return &Selector{
		Prefix:     s.Prefix,
		Columns:    s.Columns,
		Criteria:   s.Criteria,
		Limit:      s.Limit,
		Offset:     s.Offset,
		CaseFormat: s.CaseFormat,
		OmitEmpty:  s.OmitEmpty,
		selected:   s.selected,
	}
}

//Apply applies selector values
func (s *Selector) Apply(bindings map[string]interface{}) {
	if value, ok := bindings[s.Prefix+shared.FieldsKey]; ok {
		if fields := toolbox.AsString(value); value != "" {
			s.Columns = asStringSlice(fields)
			s.selected = make(map[string]bool)
			for _, column := range s.Columns {
				s.selected[column] = true
			}
		}
	}
	if value, ok := bindings[s.Prefix+shared.OrderByKey]; ok {
		s.OrderBy = toolbox.AsString(value)
	}

	if value, ok := bindings[s.Prefix+shared.CriteriaKey]; ok {
		if s.Criteria == nil {
			s.Criteria = &data.Criteria{}
		}
		s.Criteria.Expression = toolbox.AsString(value)
		if value, ok := bindings[s.Prefix+shared.ParamsKey]; ok {
			if fields := toolbox.AsString(value); value != "" {
				s.Criteria.Params = asStringSlice(fields)
			}
		}
	}
	if value, ok := bindings[s.Prefix+shared.LimitKey]; ok {
		s.Limit = toolbox.AsInt(value)
	}
	if value, ok := bindings[s.Prefix+shared.OffsetKey]; ok {
		s.Offset = toolbox.AsInt(value)
	}
}

//IsSelected returns true if supplied column matched selector.columns or selector has no specified columns.
func (s *Selector) IsSelected(columns []string) bool {
	if len(s.selected) == 0 { //no filter all selected
		return true
	}
	for _, column := range columns {
		if !s.selected[column] {
			return false
		}
	}
	return true
}

//SetColumns filters passed columns by ExcludedColumns and sets Selector Columns
func (s *Selector) SetColumns(columns []*Column) {
	excludedMap := s.excludedColumnsAsMap()

	columnsLen := len(columns)
	s.Columns = make([]string, columnsLen-len(excludedMap))
	counter := 0
	for i := 0; i < columnsLen; i++ {
		title := strings.Title(columns[i].Name)
		if _, ok := excludedMap[title]; ok {
			continue
		}
		s.Columns[counter] = columns[i].Name
		counter++
	}
}

func (s *Selector) excludedColumnsAsMap() map[string]bool {
	if s.excludedColumns != nil {
		return s.excludedColumns
	}

	excluded := make(map[string]bool)
	excludedLen := len(s.ExcludedColumns)
	for i := 0; i < excludedLen; i++ {
		excluded[strings.Title(s.ExcludedColumns[i])] = true
	}
	s.excludedColumns = excluded
	return excluded
}

func asStringSlice(text string) []string {
	var result = make([]string, 0)
	items := strings.Split(text, ",")
	for _, item := range items {
		result = append(result, strings.TrimSpace(item))
	}
	return result
}
