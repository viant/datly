package csv

import "github.com/viant/xunsafe"

type (
	Index struct {
		positionInSlice map[interface{}]int
		data            []interface{}
		objectIndex     ObjectIndex
		appenders       map[interface{}]*xunsafe.Appender
	}

	ObjectIndex   map[interface{}]PresenceIndex
	PresenceIndex map[interface{}]bool
)

func (i *Index) Has(owner, value interface{}) bool {
	presenceIndex := i.objectIndex.Index(owner)
	ok := presenceIndex[value]
	if ok {
		return true
	}

	presenceIndex[value] = true
	return false
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
