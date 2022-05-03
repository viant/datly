package data

import (
	"reflect"
)

//Selectors represents Selector registry
type Selectors map[string]*Selector

//Lookup returns and initializes Selector attached to View. Creates new one if doesn't exist.
func (s Selectors) Lookup(view *View) *Selector {
	selector, ok := s[view.Name]
	if !ok {
		selector = &Selector{}
		s[view.Name] = selector
	}

	if selector.Parameters.Values == nil {
		selector.Parameters.Values = reflect.New(view.Template.Schema.Type()).Elem().Interface()
	}

	return selector
}

//Init initializes each Selector
func (s Selectors) Init() {
	for _, selector := range s {
		selector.Init()
	}
}
