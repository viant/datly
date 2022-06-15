package view

import (
	"github.com/viant/toolbox/format"
	"strings"
)

//Selector allows customizing view fetched from Database
type (
	Selector struct {
		DatabaseFormat format.Case
		OutputFormat   format.Case
		Columns        []string   `json:",omitempty"`
		Fields         []string   `json:",omitempty"`
		OrderBy        string     `json:",omitempty"`
		Offset         int        `json:",omitempty"`
		Limit          int        `json:",omitempty"`
		Parameters     ParamState `json:",omitempty"`
		_columnNames   map[string]bool
		Criteria       string        `json:",omitempty"`
		Placeholders   []interface{} `json:",omitempty"`
		initialized    bool
	}

	ParamState struct {
		Values interface{} `json:",omitempty"`
		Has    interface{} `json:",omitempty"`
	}
)

//Init initializes Selector
func (s *Selector) Init() {
	if s.initialized {
		return
	}

	s._columnNames = Names(s.Columns).Index()
}

//Has checks if Field is present in Selector.Columns
func (s *Selector) Has(field string) bool {
	_, ok := s._columnNames[field]
	return ok
}

func (s *Selector) Add(fieldName string, isHolder bool) {
	toLower := strings.ToLower(fieldName)
	if _, ok := s._columnNames[toLower]; ok {
		return
	}

	s._columnNames[toLower] = true
	s._columnNames[fieldName] = true

	if isHolder {
		s.Fields = append(s.Fields, fieldName)
		s.Columns = append(s.Columns, fieldName)
	} else {
		s.Columns = append(s.Columns, s.OutputFormat.Format(fieldName, s.DatabaseFormat))
		s.Fields = append(s.Fields, s.OutputFormat.Format(fieldName, format.CaseUpperCamel))
	}
}
