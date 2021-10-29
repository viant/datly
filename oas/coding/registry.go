package coding

import (
	"fmt"
	"sync"
)


var _typeRegistry = sync.Map{}

//RegisterType register service for a supplied operation
func RegisterDecoder(aType string, decoder Decoder) {
	_typeRegistry.Store(aType, decoder)
}

//Lookup lookup a service for supplied operationID
func Lookup(operationID string) (Decoder, error) {
	value, ok := _typeRegistry.Load(operationID)
	if !ok {
		return nil, fmt.Errorf("failed to lookup operation for: %v", operationID)
	}
	return value.(Decoder), nil
}
