package view

import (
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
)

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
