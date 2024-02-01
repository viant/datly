package state

import (
	"net/url"
	"sync"
)

type Form struct {
	url.Values
	mux sync.RWMutex
}

// Set safely sets the value for a key.
func (f *Form) Set(key, value string) {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.Values.Set(key, value)
}

// Get safely gets the first value associated with the given key.
func (f *Form) Get(key string) string {
	f.mux.RLock()
	defer f.mux.RUnlock()
	return f.Values.Get(key)
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
