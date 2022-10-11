package view

import (
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"sync"
)

//Selector allows customizing view fetched from Database
type (
	Selector struct {
		DatabaseFormat format.Case
		OutputFormat   format.Case
		Columns        []string      `json:",omitempty"`
		Fields         []string      `json:",omitempty"`
		OrderBy        string        `json:",omitempty"`
		Offset         int           `json:",omitempty"`
		Limit          int           `json:",omitempty"`
		Parameters     ParamState    `json:",omitempty"`
		Criteria       string        `json:",omitempty"`
		Placeholders   []interface{} `json:",omitempty"`
		Page           int

		initialized  bool
		_columnNames map[string]bool
		result       *cache.Index
	}

	ParamState struct {
		Values interface{} `json:",omitempty"`
		Has    interface{} `json:",omitempty"`
	}
)

func (s *Selector) CurrentLimit() int {
	return s.Limit
}

func (s *Selector) CurrentOffset() int {
	return s.Offset
}

func (s *Selector) CurrentPage() int {
	return s.Page
}

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

//Selectors represents Selector registry
type Selectors struct {
	Index map[string]*Selector
	sync.RWMutex
}

//Lookup returns and initializes Selector attached to View. Creates new one if doesn't exist.
func (s *Selectors) Lookup(view *View) *Selector {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	if len(s.Index) == 0 {
		s.Index = map[string]*Selector{}
	}
	selector, ok := s.Index[view.Name]
	if !ok {
		selector = NewSelector()
		s.Index[view.Name] = selector
	}
	selector.Parameters.Init(view)
	return selector
}

func NewSelector() *Selector {
	return &Selector{
		_columnNames: map[string]bool{},
		initialized:  true,
	}
}

func (s *ParamState) Init(view *View) {
	if s.Values == nil {
		s.Values = newValue(view.Template.Schema.Type())
		s.Has = newValue(view.Template.PresenceSchema.Type())
	}
}

func newValue(p reflect.Type) interface{} {
	if p.Kind() == reflect.Ptr {
		return reflect.New(p.Elem()).Interface()
	}
	result := reflect.New(p)

	ptr := xunsafe.ValuePointer(&result)
	//initialise pointers
	//if struct has one filed, go returns value of the first _field, if pointer it would return nil
	//	//to workaround we initialise value of the struct
	for i := 0; i < p.NumField(); i++ {
		field := p.Field(i)
		if field.Type.Kind() == reflect.Ptr {
			newValue := reflect.New(field.Type.Elem()).Interface()
			xField := xunsafe.NewField(field)
			xField.SetValue(ptr, newValue)
		}
	}
	ret := result.Elem().Interface()
	return ret
}

//Init initializes each Selector
func (s *Selectors) Init() {
	s.RWMutex.Lock()
	s.RWMutex.Unlock()
	for _, selector := range s.Index {
		selector.Init()
	}
}
