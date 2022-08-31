package shared

import "sync"

//Errors collect errors, supports parallel errors collecting.
type Errors struct {
	locker sync.Mutex
	errors []error
}

//NewErrors creates and allocates errors collector with given size
func NewErrors(size int) *Errors {
	return &Errors{
		locker: sync.Mutex{},
		errors: make([]error, size),
	}
}

//AddError add error on given index
func (r *Errors) AddError(err error, index int) {
	r.errors[index] = err
}

//Append appends error.
func (r *Errors) Append(err error) {
	r.locker.Lock()
	defer r.locker.Unlock()
	r.errors = append(r.errors, err)
}

//Error returns first encounter error if any
func (r *Errors) Error() error {
	r.locker.Lock()
	defer r.locker.Unlock()

	for i := range r.errors {
		if r.errors[i] != nil {
			return r.errors[i]
		}
	}

	return nil
}
