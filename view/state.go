package view

import (
	"strings"
	"sync"

	"github.com/viant/datly/view/state/predicate"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/structology"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/handler/state"
)

// Statelet allows customizing View fetched from Database
type (

	//InputType represents view state
	Statelet struct {
		//SELECTORS
		DatabaseFormat text.CaseFormat
		OutputFormat   text.CaseFormat
		Template       *structology.State
		state.QuerySelector
		QuerySettings
		filtersMu    sync.Mutex
		initialized  bool
		_columnNames map[string]bool
		result       *cache.ParmetrizedQuery
		predicate.Filters
		Ignore bool
	}

	QuerySettings struct {
		//SETTINGS
		SyncFlag      bool
		ContentFormat string
	}
)

// Init initializes Statelet
func (s *Statelet) Init(aView *View) {
	if aView != nil && s.Template == nil && aView.Template != nil && aView.Template.stateType != nil {
		s.Template = aView.Template.stateType.NewState()
	}
	if s.initialized {
		return
	}
	s._columnNames = Names(s.Columns).Index()
}

// Has checks if Field is present in Template.Columns
func (s *Statelet) Has(field string) bool {
	_, ok := s._columnNames[field]
	return ok
}

func (s *Statelet) Add(fieldName string, isHolder bool) {
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
		s.Fields = append(s.Fields, s.OutputFormat.Format(fieldName, text.CaseFormatUpperCamel))
	}
}

// AppendFilters safely appends filters to the selector's Filters to avoid data races.
func (s *Statelet) AppendFilters(filters predicate.Filters) {
	if len(filters) == 0 {
		return
	}
	s.filtersMu.Lock()
	s.Filters = append(s.Filters, filters...)
	s.filtersMu.Unlock()
}

// NewStatelet creates a selector
func NewStatelet() *Statelet {
	return &Statelet{
		_columnNames: map[string]bool{},
		initialized:  true,
	}
}

// State represents view statelet registry
type State struct {
	Views map[string]*Statelet
	sync.RWMutex
}

// QuerySelector returns query selector
func (s *State) QuerySelector(view *View) *state.QuerySelector {
	statelet := s.Lookup(view)
	if statelet == nil {
		return nil
	}
	return &statelet.QuerySelector
}

// QuerySettings returns query settings
func (s *State) QuerySettings(view *View) *QuerySettings {
	statelet := s.Lookup(view)
	if statelet == nil {
		return nil
	}
	return &statelet.QuerySettings
}

// Lookup returns and initializes Statelet attached to View. Creates new one if doesn't exist.
func (s *State) Lookup(view *View) *Statelet {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	if len(s.Views) == 0 {
		s.Views = map[string]*Statelet{}
	}
	selector, ok := s.Views[view.Name]
	if !ok {
		selector = NewStatelet()
		s.Views[view.Name] = selector
	}

	selector.Init(view)
	return selector
}

// NewState creates a selector
func NewState() *State {
	return &State{
		Views:   map[string]*Statelet{},
		RWMutex: sync.RWMutex{},
	}
}

// Init initializes each Statelet
func (s *State) Init(aView *View) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	for _, selector := range s.Views {
		selector.Init(aView)
	}
}

func (s *Statelet) IgnoreRead() {
	s.Ignore = true
}
