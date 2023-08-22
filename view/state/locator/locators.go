package locator

import (
	"fmt"
	"github.com/viant/datly/view/state"
	"sync"
)

// Locators represents locators
type Locators struct {
	sync.RWMutex
	byKind  map[state.Kind]state.Locator
	parent  *Locators
	options []Option
}

// Lookup return locator for supplied kind or error
func (r *Locators) Lookup(kind state.Kind) (state.Locator, error) {
	r.RWMutex.RLock()
	locator, ok := r.byKind[kind]
	r.RWMutex.RUnlock()
	if ok {
		return locator, nil
	}
	if r.parent == nil {
		return nil, fmt.Errorf("failed to lookup locator for kind: %v", kind)
	}
	var err error
	locator, err = r.parent.Lookup(kind)
	if err != nil {
		if newLocator := Lookup(kind); newLocator != nil {
			if locator, err = newLocator(r.options...); err != nil {
				return nil, err
			}
			r.RWMutex.Lock()
			r.byKind[kind] = locator
			r.RWMutex.Unlock()
		}
	}
	return locator, err
}

// NewLocators creates a locator
func NewLocators(parent *Locators, options ...Option) *Locators {
	ret := &Locators{
		byKind:  make(map[state.Kind]state.Locator),
		parent:  parent,
		options: options,
	}
	return ret
}
