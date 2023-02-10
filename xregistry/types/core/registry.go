package core

import (
	"reflect"
	"sync"
	"time"
)

func RegisterType(packageName, typeName string, rType reflect.Type, insertedAt time.Time) {
	instance.register(packageName, typeName, rType, insertedAt)
}

func Types(callback NotifierFn) (types map[string]map[string]reflect.Type, closeFn func()) {
	return instance.types(callback)
}

var instance = &registry{
	packages:  map[string]*packageRegistry{},
	notifiers: map[int64]NotifierFn{},
}

type (
	NotifierFn func(packageName, typeName string, rType reflect.Type, insertedAt time.Time)

	registry struct {
		sync.Mutex
		packages  map[string]*packageRegistry
		notifiers map[int64]NotifierFn
	}

	packageRegistry struct {
		index map[string]*entry
	}

	entry struct {
		inserted time.Time
		rType    reflect.Type
	}
)

func (r *packageRegistry) register(typeName string, rType reflect.Type, insertedAt time.Time) bool {
	anEntry, ok := r.index[typeName]
	if !ok {
		anEntry = &entry{
			inserted: insertedAt,
			rType:    rType,
		}
		r.index[typeName] = anEntry
		return false
	}

	if anEntry.rType == rType || anEntry.inserted.After(insertedAt) {
		return false
	}

	anEntry.rType = rType
	anEntry.inserted = insertedAt
	return true
}

func (r *registry) register(packageName, typeName string, rType reflect.Type, insertedAt time.Time) {
	r.Lock()
	defer r.Unlock()

	index := r.packageRegistry(packageName)
	if index.register(typeName, rType, insertedAt) {
		for _, notifier := range r.notifiers {
			notifier(packageName, typeName, rType, insertedAt)
		}
	}
}

func (r *registry) packageRegistry(name string) *packageRegistry {
	index, ok := r.packages[name]
	if ok {
		return index
	}

	index = &packageRegistry{index: map[string]*entry{}}
	r.packages[name] = index
	return index
}

func (r *registry) types(callback NotifierFn) (map[string]map[string]reflect.Type, func()) {
	r.Lock()
	defer r.Unlock()

	result := map[string]map[string]reflect.Type{}
	for packageName, pRegistry := range r.packages {
		tRegistry := map[string]reflect.Type{}
		result[packageName] = tRegistry
		for tName, tEntry := range pRegistry.index {
			tRegistry[tName] = tEntry.rType
		}
	}

	if callback == nil {
		return result, func() {}
	}

	now := time.Now().Unix()
	for {
		if _, ok := r.notifiers[now]; ok {
			now++
		} else {
			r.notifiers[now] = callback
			return result, func() {
				r.Lock()
				defer r.Unlock()
				delete(r.notifiers, now)
			}
		}
	}
}
