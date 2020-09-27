package db

import (
	"github.com/viant/gtly"
	"github.com/viant/toolbox"
)

type insertable struct {
	indexer    *Indexer
	index      map[string][]interface{}
	collection gtly.Collection
}

func (i *insertable) Range(handler func(item interface{}) (bool, error)) error {
	return i.collection.Objects(func(item *gtly.Object) (toContinue bool, err error) {
		if len(i.index) == 0 {
			return handler(item)
		}
		key, hasKey := i.indexer.Key(item)
		_, hasIndexKey := i.index[key]
		if !hasKey || !hasIndexKey {
			return handler(item)
		}
		return true, nil
	})
}

//NewInsertable creates an insertable collection
func NewInsertable(collection gtly.Collection, indexer *Indexer, index map[string][]interface{}) toolbox.Ranger {
	return &insertable{
		indexer:    indexer,
		index:      index,
		collection: collection,
	}
}
