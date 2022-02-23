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
