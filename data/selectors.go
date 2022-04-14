package data

//Selectors represents Selector registry
type Selectors map[string]*Selector

//Lookup returns Selector attached to View
func (s Selectors) Lookup(viewName string) *Selector {
	return s[viewName]
}

//Init initializes each Selector
func (s Selectors) Init() {
	for _, selector := range s {
		selector.Init()
	}
}

func (s Selectors) GetOrCreate(viewName string) *Selector {
	selector, ok := s[viewName]
	if ok {
		return selector
	}

	selector = &Selector{}
	s[viewName] = selector

	return selector
}
