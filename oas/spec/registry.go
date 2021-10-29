package spec

import (
	"fmt"
	"sync"
)




var _typeRegistry = sync.Map{}

//RegisterType register provider for a supplied type
func RegisterType(aType string, provider Provider) {
	_typeRegistry.Store(aType, provider)
}

//LookupType lookup a provider for supplied type
func LookupType(aType string) (Provider, error) {
	value, ok := _typeRegistry.Load(aType)
	if !ok {
		return nil, fmt.Errorf("failed to lookup provider for: %v", aType)
	}
	return value.(Provider), nil
}
