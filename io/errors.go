package io

import (
	"fmt"
	"sync"
)

//Errors represents error container
type Errors struct {
	mux sync.RWMutex
	errors []error
}


//Errors returns errors
func (e *Errors) Errors() []error {
	e.mux.RLock()
	defer e.mux.RUnlock()
	result := e.errors
	return result
}


func (e *Errors) Add(err error) {
	if err ==nil {
		return
	}
	e.mux.Lock()
	defer e.mux.Unlock()
	e.errors = append(e.errors, err)
}

func (e *Errors) IsEmpty() bool {
	result := e.errors
	e.mux.RLock()
	defer e.mux.RUnlock()
	return len(result) == 0
}

func (e *Errors) Error() string {
	e.mux.RLock()
	defer e.mux.RUnlock()
	if len(e.errors) == 0 {
		return ""
	}
	err := e.errors[0]
	for i:=1;i<len(e.errors);i++ {
		err = fmt.Errorf("%w, %v", err, e.errors[i])
	}
	return err.Error()
}

