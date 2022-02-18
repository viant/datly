package db

import (
	"github.com/viant/gtly"
	"github.com/viant/toolbox"
)

type updatable struct {
	indexer    *Indexer
	index      map[string][]interface{}
	collection gtly.Collection
}

func (i *updatable) Range(handler func(item interface{}) (bool, error)) error {
	return i.collection.Objects(func(item *gtly.Object) (toContinue bool, err error) {
		key, hasKey := i.indexer.Key(item)
		_, hasIndexKey := i.index[key]
		if !hasKey || !hasIndexKey {
			return true, nil
		}
		return handler(item)
	})
}

//Newupdatable creates an updatable collection
func Newupdatable(collection gtly.Collection, indexer *Indexer, index map[string][]interface{}) toolbox.Ranger {
	return &updatable{
		indexer:    indexer,
		index:      index,
		collection: collection,
	}
}
