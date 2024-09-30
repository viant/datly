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
	opts    *Options
}

func (l *KindLocator) RemoveLocators(kind ...state.Kind) {
	if len(kind) == 0 {
		return
	}
	l.RWMutex.Lock()
	defer l.RWMutex.Unlock()
	for _, k := range kind {
		delete(l.byKind, k)
	}
}

func (l *KindLocator) Options() []Option {
	var ret []Option
	if l.opts.ReadInto != nil {
		ret = append(ret, WithReadInto(l.opts.ReadInto))
	}
	if l.opts.Dispatcher != nil {
		ret = append(ret, WithDispatcher(l.opts.Dispatcher))
	}
	if l.opts.Unmarshal != nil {
		ret = append(ret, WithUnmarshal(l.opts.Unmarshal))
	}
	if l.opts.Custom != nil {
		ret = append(ret, WithCustom(l.opts.Custom...))
	}
	return l.options
}

// With creates sub locator with options
func (l *KindLocator) With(options ...Option) *KindLocator {
	opts := ensureParentOptions(l, options)
	return NewKindsLocator(l, opts...)
}

func (l *KindLocator) Has(kind state.Kind) bool {
	if l == nil {
		return false
	}
	l.RWMutex.RLock()
	_, ok := l.byKind[kind]
	l.RWMutex.RUnlock()
	return ok
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
	if !l.parent.Has(kind) {
		if locator, err = l.registerLocator(kind); err != nil {
			return nil, fmt.Errorf("failed to lookup locator for kind: %v, %w", kind, err)
		}
		return locator, nil
	}
	return l.parent.Lookup(kind)
}

func (l *KindLocator) registerLocator(kind state.Kind) (kind.Locator, error) {
	if newLocator := Lookup(kind); newLocator != nil {
		l.RWMutex.RLock()
		ret, ok := l.byKind[kind]
		l.RWMutex.RUnlock()
		if ok {
			return ret, nil
		}
		locator, err := newLocator(l.options...)
		if err != nil {
			return nil, err
		}
		l.RWMutex.Lock()
		l.byKind[kind] = locator
		l.RWMutex.Unlock()
		return locator, nil
	}
	return nil, fmt.Errorf("unsupported kind: %v", kind)
}

// NewKindsLocator creates a locator
func NewKindsLocator(parent *KindLocator, options ...Option) *KindLocator {
	ret := &KindLocator{
		byKind: make(map[state.Kind]kind.Locator),
		parent: parent,
	}
	ret.options = ensureParentOptions(ret, options)
	ret.opts = NewOptions(ret.options)
	return ret
}

func ensureParentOptions(parent *KindLocator, options []Option) []Option {
	opts := append(parent.options, WithParent(parent))
	opts = append(opts, options...)
	return opts
}
