package httputils

import (
	"net/http"
	"reflect"
	"sync"
)

const (
	priorityDefault = iota
	priority400
	priority404
	priority403
	priority401
)

type (
	Error struct {
		View       string      `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		Parameter  string      `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		StatusCode int         `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		Err        error       `json:"-"`
		Message    string      `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		Object     interface{} `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
	}

	Option func(e *Error)

	Errors struct {
		Message string `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		Errors  []*Error
		mutex   sync.Mutex
		status  int
	}
)

func WithObject(object interface{}) Option {
	return func(e *Error) {
		if object == nil {
			return
		}
		objectType := reflect.TypeOf(object)
		if objectType.Kind() == reflect.Ptr {
			objectType = objectType.Elem()
		}
		switch objectType.Kind() {
		case reflect.Struct, reflect.Slice:
			e.Object = object
		}
	}
}

func WithMessage(msg string) Option {
	return func(e *Error) {
		e.Message = msg
	}
}

func WithStatusCode(code int) Option {
	return func(e *Error) {
		e.StatusCode = code
	}
}

func (e *Errors) ErrorStatusCode() int {
	return e.status
}

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Err.Error()
}

func NewErrors() *Errors {
	return &Errors{mutex: sync.Mutex{}}
}

func (e *Errors) HasError() bool {
	e.mutex.Lock()
	ret := len(e.Errors)
	e.mutex.Unlock()
	return ret > 0
}

func (e *Errors) Append(err error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	switch actual := err.(type) {
	case *Error:
		e.Errors = append(e.Errors, actual)
		e.updateStatusCode(actual.StatusCode)
	case *Errors:
		e.Errors = append(e.Errors, actual.Errors...)
		code := actual.ErrorStatusCode()
		e.updateStatusCode(code)
	default:
		e.Errors = append(e.Errors, &Error{Message: err.Error(), Err: err})
	}
}

func (e *Errors) updateStatusCode(code int) {
	if statusCodePriority(code) > statusCodePriority(e.status) {
		e.status = code
	}
}

func (e *Errors) AddError(view, param string, err error, opts ...Option) {
	if err == nil {
		return
	}

	e.mutex.Lock()
	e.Errors = append(e.Errors, NewParamError(view, param, err, opts...))
	e.Message = err.Error()
	e.mutex.Unlock()
}

func NewParamError(view string, param string, err error, opts ...Option) *Error {
	ret := &Error{
		View:      view,
		Parameter: param,
		Err:       err,
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (e *Errors) Error() string {
	if e.Message == "" {
		if len(e.Errors) > 0 {
			return e.Errors[0].Error()
		}
	}
	return e.Message
}

func (e *Errors) SetStatus(code int) {
	if code == 0 {
		return
	}

	e.mutex.Lock()
	if statusCodePriority(code) > statusCodePriority(e.status) {
		e.status = code
	}

	e.mutex.Unlock()
}

func statusCodePriority(status int) int {
	switch status {
	case http.StatusUnauthorized:
		return priority401
	case http.StatusForbidden:
		return priority403
	case http.StatusNotFound:
		return priority404
	case http.StatusBadRequest:
		return priority400
	case 0:
		return -1
	default:
		return priorityDefault
	}
}
