package gmetricx

import "github.com/viant/gmetric"

// LookupOperation returns a snapshot of the named operation under the gmetricx service lock.
func LookupOperation(service *gmetric.Service, name string) *gmetric.Operation {
	if service == nil {
		return nil
	}
	mux := serviceLock(service)
	mux.Lock()
	defer mux.Unlock()
	if op := lookupOperationUnlocked(service, name); op != nil {
		snapshot := *op
		return &snapshot
	}
	return nil
}
