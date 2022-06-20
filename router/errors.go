package router

import (
	"bytes"
	goJson "encoding/json"
	"github.com/go-playground/validator"
	"github.com/viant/toolbox"
	"sync"
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
		Errors []*Error
		mutex  sync.Mutex
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
	asBytes, _ := goJson.Marshal(e)
	return string(asBytes)
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
