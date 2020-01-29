package db

import (
	"github.com/viant/datly/generic"
	"github.com/viant/toolbox"
)

type insertable struct {
	indexer    *Indexer
	index      map[string][]interface{}
	collection generic.Collection
}

func (i *insertable) Range(handler func(item interface{}) (bool, error)) error {
	return i.collection.Objects(func(item *generic.Object) (toContinue bool, err error) {
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
func NewInsertable(collection generic.Collection, indexer *Indexer, index map[string][]interface{}) toolbox.Ranger {
	return &insertable{
		indexer:    indexer,
		index:      index,
		collection: collection,
	}
}
