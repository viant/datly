package predicate

// Filter represents predicate instance
type Filter struct {
	Name    string
	Include []interface{}
	Exclude []interface{}
}

// Filters represents a filter collection
type Filters []*Filter

// Lookup lookups filter
func (f *Filters) Lookup(name string) *Filter {
	for _, candidate := range *f {
		if name == candidate.Name {
			return candidate
		}
	}
	return nil
}

// Add adds filter with provided name
func (f *Filters) Add(name string) *Filter {
	ret := &Filter{Name: name}
	*f = append(*f, ret)
	return ret
}

// LookupOrAdd lookup or add filter for specified name
func (f *Filters) LookupOrAdd(name string) *Filter {
	ret := f.Lookup(name)
	if ret == nil {
		ret = f.Add(name)
	}
	return ret
}
