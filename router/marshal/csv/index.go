package csv

type (
	Index struct {
		positionInSlice map[interface{}]int
		data            []interface{}
		pathIndex       ChildPathIndex
	}

	ChildPathIndex map[string]ObjectIndex
	ObjectIndex    map[interface{}]PresenceIndex
	PresenceIndex  map[interface{}]bool
)

func (i *Index) Has(path string, owner, value interface{}) bool {
	index := i.pathIndex.Index(path)
	presenceIndex := index.Index(owner)
	ok := presenceIndex[value]
	if ok {
		return true
	}

	presenceIndex[value] = true
	return false
}

func (p ChildPathIndex) Index(key string) ObjectIndex {
	index, ok := p[key]
	if ok {
		return index
	}

	index = ObjectIndex{}
	p[key] = index
	return index
}

func (i ObjectIndex) Index(value interface{}) PresenceIndex {
	index, ok := i[value]
	if ok {
		return index
	}

	index = PresenceIndex{}
	i[value] = index
	return index
}

func (p PresenceIndex) Has(value interface{}) bool {
	return p[value]
}

func (i *Index) Get(key string) (interface{}, bool) {
	value, ok := i.positionInSlice[key]
	return value, ok
}
