package locator

import (
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"sync"
)

// KindLocator represents locators
type KindLocator struct {
	sync.RWMutex
	byKind  map[state.Kind]kind.Locator
	parent  *KindLocator
	options []Option
}

// With creates sub locator with options
func (l *KindLocator) With(options ...Option) *KindLocator {
	opts := ensureParentOptions(l, options)
	return NewKindsLocator(l, opts...)
}

// Lookup return locator for supplied kind or error
func (l *KindLocator) Lookup(kind state.Kind) (kind.Locator, error) {
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

func (l *KindLocator) registerLocator(kind state.Kind, locator kind.Locator) (kind.Locator, error) {
	var err error
	if newLocator := Lookup(kind); newLocator != nil {
		l.RWMutex.Lock()
		if ret, ok := l.byKind[kind]; ok {
			return ret, nil
		}
		if locator, err = newLocator(l.options...); err != nil {
			return nil, err
		}
		l.byKind[kind] = locator
		l.RWMutex.Unlock()
	}
	return locator, nil
}

// NewKindsLocator creates a locator
func NewKindsLocator(parent *KindLocator, options ...Option) *KindLocator {
	ret := &KindLocator{
		byKind: make(map[state.Kind]kind.Locator),
		parent: parent,
	}
	ret.options = ensureParentOptions(ret, options)
	return ret
}

func ensureParentOptions(parent *KindLocator, options []Option) []Option {
	opts := append([]Option{}, WithParent(parent))
	opts = append(opts, options...)
	return opts
}
