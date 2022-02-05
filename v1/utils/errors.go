package utils

import "sync"

type Errors struct {
	locker    sync.Mutex
	errors    []error
	isInvalid bool
}

func NewErrors(size int) *Errors {
	return &Errors{
		locker:    sync.Mutex{},
		errors:    make([]error, size),
		isInvalid: false,
	}
}

func (r *Errors) AddError(err error, index int) {
	r.locker.Lock()
	defer r.locker.Unlock()

	r.errors[index] = err
	r.isInvalid = r.isInvalid || err != nil
}

func (r *Errors) Error() error {
	if !r.isInvalid {
		return nil
	}

	for i := range r.errors {
		if r.errors[i] != nil {
			return r.errors[i]
		}
	}

	return nil
}
