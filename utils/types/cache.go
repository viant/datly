package types

import (
	"github.com/viant/xreflect"
	"reflect"
	"sync"
)

type (
	Cache struct {
		cache       sync.Map
		typesLookup xreflect.TypeLookupFn
	}

	key struct {
		pkgId    string
		pkgName  string
		typeName string
	}
)

func NewCache(lookup xreflect.TypeLookupFn) *Cache {
	return &Cache{
		cache:       sync.Map{},
		typesLookup: lookup,
	}
}

func (c *Cache) LoadType(pkgIdentifier string, pkgName string, typeName string) (reflect.Type, error) {
	aKey := key{
		pkgId:    pkgIdentifier,
		pkgName:  pkgName,
		typeName: typeName,
	}

	value, ok := c.cache.Load(aKey)
	if ok {
		return value.(reflect.Type), nil
	}

	parseType, err := GetOrParseType(c.typesLookup, typeName)
	if err == nil {
		c.cache.Store(aKey, parseType)
	}

	return parseType, err
}
