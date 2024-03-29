package httputils

import (
	"github.com/viant/xdatly/handler/http"
)

func BuildErrorResponse(err error) (statusCode int, errorMessage string) {
	statusCode = 400
	errorMessage = ""

	messager, ok := err.(http.ErrorMessager)
	if ok {
		errorMessage = messager.ErrorMessage()
	}

	coder, ok := err.(http.ErrorStatusCoder)
	if ok {
		statusCode = coder.ErrorStatusCode()
	}

	return statusCode, errorMessage
}

type HttpMessageError struct {
	statusCode int
	err        error
}

func NewHttpStatusError(statusCode int) *HttpMessageError {
	return &HttpMessageError{statusCode: statusCode}
}
func NewHttpMessageError(statusCode int, err error) *HttpMessageError {
	return &HttpMessageError{
		statusCode: statusCode,
		err:        err,
	}
}

func ErrorOf(defaultStatusCode int, err error) *HttpMessageError {
	if err != nil {
		coder, ok := err.(http.ErrorStatusCoder)
		if ok {
			defaultStatusCode = coder.ErrorStatusCode()
		}
	}

	return NewHttpMessageError(defaultStatusCode, err)
}

func (h *HttpMessageError) ErrorMessage() string {
	if h.err == nil {
		return ""
	}

	return h.err.Error()
}

func (h *HttpMessageError) ErrorStatusCode() int {
	return h.statusCode
}

func (h *HttpMessageError) Err() error {
	return h.err
}

func (h *HttpMessageError) Error() string {
	if h.err == nil {
		return ""
	}

	return h.err.Error()
}
