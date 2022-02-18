package data

type Selectors map[string]*Selector

func (s Selectors) Lookup(selector string) *Selector {
	return s[selector]
}

func (s Selectors) Init() {
	for _, selector := range s {
		selector.Init()
	}
}
