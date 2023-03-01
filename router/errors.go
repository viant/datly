package router

import (
	"bytes"
	"github.com/go-playground/validator"
	"github.com/viant/toolbox"
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
		View    string      `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		Param   string      `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		Err     error       `json:"-"`
		Message string      `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		Object  interface{} `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
	}

	Errors struct {
		Message string `json:",omitempty" default:"nullable=true,required=false,allowEmpty=true"`
		Errors  []*Error
		mutex   sync.Mutex
		status  int
	}

	ParamErrors []*ParamError
	ParamError  struct {
		Value interface{}
		Field string
		Tag   string
	}
)

func (e *Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Err.Error()
}

func NewErrors() *Errors {
	return &Errors{mutex: sync.Mutex{}}
}

func (e *Errors) AddError(view, param string, err error) {
	e.mutex.Lock()
	e.Errors = append(e.Errors, &Error{
		View:  view,
		Param: param,
		Err:   err,
	})
	e.mutex.Unlock()
}

func (e *Errors) Error() string {
	return e.Message
}

func (e *Errors) setStatus(code int) {
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
	case 401:
		return priority401
	case 403:
		return priority403
	case 404:
		return priority404
	case 400:
		return priority400
	case 0:
		return -1
	default:
		return priorityDefault
	}
}

func NewParamErrors(validationErrors validator.ValidationErrors) ParamErrors {
	paramErrors := make([]*ParamError, len(validationErrors))
	for i, validationError := range validationErrors {
		paramErrors[i] = &ParamError{
			Value: validationError.Value(),
			Field: validationError.StructNamespace(),
			Tag:   validationError.Tag(),
		}
	}

	return paramErrors
}

func (p ParamErrors) Error() string {
	bufferString := bytes.NewBufferString("")
	for i, paramError := range p {
		if i != 0 {
			bufferString.WriteByte(',')
		}
		bufferString.WriteString("invalid property ")
		bufferString.WriteString(paramError.Field)
		bufferString.WriteString(" value ")
		bufferString.WriteString(toolbox.AsString(paramError.Value))
		bufferString.WriteString(" due to tue ")
		bufferString.WriteString(paramError.Tag)
	}

	return bufferString.String()
}
