package shared

import "sync"

type Errors struct {
	locker sync.Mutex
	errors []error
}

func NewErrors(size int) *Errors {
	return &Errors{
		locker: sync.Mutex{},
		errors: make([]error, size),
	}
}

func (r *Errors) AddError(err error, index int) {
	r.errors[index] = err
}

func (r *Errors) Append(err error) {
	r.locker.Lock()
	defer r.locker.Unlock()
	r.errors = append(r.errors, err)
}

func (r *Errors) Error() error {
	for i := range r.errors {
		if r.errors[i] != nil {
			return r.errors[i]
		}
	}

	return nil
}
