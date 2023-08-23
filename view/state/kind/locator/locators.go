package locator

import (
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"sync"
)

// Locators represents locators
type Locators struct {
	sync.RWMutex
	byKind  map[state.Kind]kind.Locator
	parent  *Locators
	options []Option
}

// With creates sub locator with options
func (l *Locators) With(options ...Option) *Locators {
	opts := ensureParentOptions(l, options)
	return NewLocators(l, opts...)
}

// Lookup return locator for supplied kind or error
func (l *Locators) Lookup(kind state.Kind) (kind.Locator, error) {
	l.RWMutex.RLock()
	locator, ok := l.byKind[kind]
	l.RWMutex.RUnlock()
	if ok {
		return locator, nil
	}
	var err error
	if l.parent == nil {
		if locator, err = l.registerLocator(kind, locator); err != nil {
			return nil, fmt.Errorf("failed to lookup locator for kind: %v", kind)
		}
	}
	locator, err = l.parent.Lookup(kind)
	if err != nil {
		if locator, err = l.registerLocator(kind, locator); err != nil {
			return nil, fmt.Errorf("failed to lookup locator for kind: %v", kind)
		}
	}
	return locator, err
}

func (l *Locators) registerLocator(kind state.Kind, locator kind.Locator) (kind.Locator, error) {
	var err error
	if newLocator := Lookup(kind); newLocator != nil {
		if locator, err = newLocator(l.options...); err != nil {
			return nil, err
		}
		l.RWMutex.Lock()
		l.byKind[kind] = locator
		l.RWMutex.Unlock()
	}
	return locator, nil
}

// NewLocators creates a locator
func NewLocators(parent *Locators, options ...Option) *Locators {
	ret := &Locators{
		byKind: make(map[state.Kind]kind.Locator),
		parent: parent,
	}
	ret.options = ensureParentOptions(ret, options)
	return ret
}

func ensureParentOptions(parent *Locators, options []Option) []Option {
	opts := append([]Option{}, WithParent(parent))
	opts = append(opts, options...)
	return opts
}
