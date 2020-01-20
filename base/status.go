package base

import (
	"sync"
)

//StatusInfo represents status
type StatusInfo struct {
	Status  string
	Errors  []*ErrorInfo `json:",omitempty"`
	mux     sync.Mutex
}

//ErrorInfo represents an error info
type ErrorInfo struct {
	Message string
	Location string
	Type string
}

//AddError add an error to response
func (r *StatusInfo) AddError(location, errType string, err error) {
	if err == nil {
		return
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Status = StatusError
	if len(r.Errors) == 0 {
		r.Errors = make([]*ErrorInfo, 0)
	}
	info := &ErrorInfo{
		Location:location,
		Type:errType,
		Message:err.Error(),
	}
	r.Errors = append(r.Errors, info)
}



