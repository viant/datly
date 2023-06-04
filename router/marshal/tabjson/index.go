package tabjson

import "github.com/viant/xunsafe"

type (
	Index struct {
		positionInSlice map[interface{}]int
		data            []interface{}
		objectIndex     ObjectIndex
		appenders       map[interface{}]*xunsafe.Appender
	}

	ObjectIndex map[interface{}]setMarker
	setMarker   map[interface{}]bool
)

func (i *Index) Has(owner, value interface{}) bool {
	setMarker := i.objectIndex.Index(owner)
	ok := setMarker[value]
	if ok {
		return true
	}

	setMarker[value] = true
	return false
}

func (i ObjectIndex) Index(value interface{}) setMarker {
	index, ok := i[value]
	if ok {
		return index
	}

	index = setMarker{}
	i[value] = index
	return index
}

func (p setMarker) Has(value interface{}) bool {
	return p[value]
}

func (i *Index) Get(key string) (interface{}, bool) {
	value, ok := i.positionInSlice[key]
	return value, ok
}
