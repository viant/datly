package db

import (
	"github.com/viant/datly/data"
	"github.com/viant/gtly"
	"github.com/viant/toolbox"
	"strings"
)

//Indexer represents primary key indexer
type Indexer struct {
	view *data.View
}

func (i Indexer) Key(item *gtly.Object) (string, bool) {
	var values = make([]string, len(i.view.PrimaryKey))
	for i, pk := range i.view.PrimaryKey {
		value := item.Value(pk)
		if value == nil {
			return "", false
		}
		values[i] = toolbox.AsString(value)
	}
	return strings.Join(values, "-"), true
}

func (i Indexer) Values(item *gtly.Object) []interface{} {
	var values = make([]interface{}, len(i.view.PrimaryKey))
	for i, pk := range i.view.PrimaryKey {
		values[i] = item.Value(pk)
	}
	return values
}

func (i Indexer) Index(collection gtly.Collection) map[string][]interface{} {
	var index = make(map[string][]interface{})
	collection.Objects(func(item *gtly.Object) (toContinue bool, err error) {
		key, ok := i.Key(item)
		if !ok {
			return true, nil
		}
		index[key] = i.Values(item)
		return true, nil
	})
	return index
}

//NewIndexer creates an indexer
func NewIndexer(view *data.View) *Indexer {
	return &Indexer{view: view}
}
