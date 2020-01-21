package hook

import "sync"

//Registry visitor hook registry
type Registry struct {
	registry map[string]*Visitor
	mux      sync.RWMutex
}
