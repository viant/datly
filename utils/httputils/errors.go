package httputils

import (
	"bytes"
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

func (e *Errors) AddError(view, param string, err error) {
	if err == nil {
		return
	}

	e.mutex.Lock()
	e.Errors = append(e.Errors, NewParamError(view, param, err))
	e.Message = err.Error()
	e.mutex.Unlock()
}

func NewParamError(view string, param string, err error) *Error {
	return &Error{
		View:  view,
		Param: param,
		Err:   err,
	}
}

func (e *Errors) Error() string {
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