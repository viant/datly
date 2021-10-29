package operation

import (
	"fmt"
	"sync"
)


var _typeRegistry = sync.Map{}

//RegisterType register service for a supplied operation
func Register(operationID string, service Service) {
	_typeRegistry.Store(operationID, service)
}

//Lookup lookup a service for supplied operationID
func Lookup(operationID string) (Service, error) {
	value, ok := _typeRegistry.Load(operationID)
	if !ok {
		return nil, fmt.Errorf("failed to lookup operation for: %v", operationID)
	}
	return value.(Service), nil
}
