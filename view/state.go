package view

import (
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/structology"
	"github.com/viant/toolbox/format"
	"strings"
	"sync"
)

// State allows customizing View fetched from Database
type (

	//State represents view state
	State struct {
		Template *structology.State
		QuerySelector
	}

	QuerySelector struct {
		DatabaseFormat format.Case
		OutputFormat   format.Case
		Columns        []string `json:",omitempty"`
		Fields         []string `json:",omitempty"`
		OrderBy        string   `json:",omitempty"`
		Offset         int      `json:",omitempty"`
		Limit          int      `json:",omitempty"`

		Criteria     string        `json:",omitempty"`
		Placeholders []interface{} `json:",omitempty"`
		Page         int
		Ignore       bool

		initialized  bool
		_columnNames map[string]bool
		result       *cache.ParmetrizedQuery
	}
)

func (s *QuerySelector) CurrentLimit() int {
	return s.Limit
}

func (s *QuerySelector) CurrentOffset() int {
	return s.Offset
}

func (s *QuerySelector) CurrentPage() int {
	return s.Page
}

// Init initializes State
func (s *State) Init(aView *View) {
	if aView != nil && s.Template == nil && aView.Template.stateType != nil {
		s.Template = aView.Template.stateType.NewState()
	}
	if s.initialized {
		return
	}
	s._columnNames = Names(s.Columns).Index()
}

// Has checks if Field is present in Template.Columns
func (s *QuerySelector) Has(field string) bool {
	_, ok := s._columnNames[field]
	return ok
}

func (s *QuerySelector) Add(fieldName string, isHolder bool) {
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

func (s *QuerySelector) SetCriteria(expanded string, group []interface{}) {
	s.Criteria = expanded
	s.Placeholders = group
}

// NewState creates a selector
func NewState() *State {
	return &State{
		QuerySelector: QuerySelector{
			_columnNames: map[string]bool{},
			initialized:  true,
		},
	}
}

// States represents State registry
type States struct {
	Index map[string]*State
	sync.RWMutex
}

// Lookup returns and initializes State attached to View. Creates new one if doesn't exist.
func (s *States) Lookup(view *View) *State {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	if len(s.Index) == 0 {
		s.Index = map[string]*State{}
	}
	selector, ok := s.Index[view.Name]
	if !ok {
		selector = NewState()
		s.Index[view.Name] = selector
	}

	selector.Init(view)
	return selector
}

// NewStates creates a selector
func NewStates() *States {
	return &States{
		Index:   map[string]*State{},
		RWMutex: sync.RWMutex{},
	}
}

// Init initializes each State
func (s *States) Init(aView *View) {
	s.RWMutex.Lock()
	s.RWMutex.Unlock()
	for _, selector := range s.Index {
		selector.Init(aView)
	}
}

func (s *State) IgnoreRead() {
	s.Ignore = true
}
