package state

import (
	"net/url"
	"sync"
)

type Form struct {
	url.Values
	mux sync.RWMutex
}

func (f *Form) Mutex() *sync.RWMutex {
	return &f.mux
}

// Set safely sets the value for a key.
func (f *Form) Set(key string, values ...string) {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.Values[key] = values
}

// Get safely gets the first value associated with the given key.
func (f *Form) Get(key string) string {
	f.mux.RLock()
	defer f.mux.RUnlock()
	return f.Values.Get(key)
}

// Lookup safely gets the first value associated with the given key.
func (f *Form) Lookup(key string) ([]string, bool) {
	f.mux.RLock()
	defer f.mux.RUnlock()
	ret, ok := f.Values[key]
	return ret, ok
}

// Add safely adds the value to the key.
func (f *Form) Add(key, value string) {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.Values.Add(key, value)
}

// Del safely deletes the values associated with key.
func (f *Form) Del(key string) {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.Values.Del(key)
}

func (f *Form) SetValues(values url.Values) {
	f.mux.Lock()
	defer f.mux.Unlock()
	if f.Values == nil {
		f.Values = values
		return
	}
	for key, values := range f.Values {
		f.Values[key] = values
	}
}

func NewForm() *Form {
	return &Form{
		Values: url.Values{},
	}
}
