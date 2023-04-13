package httputils

type ErrorStatusCoder interface {
	ErrorStatusCode() int
}

type ErrorMessager interface {
	ErrorMessage() string
}

type ErrorObjecter interface {
	ErrorObject() interface{}
}

func BuildErrorResponse(err error) (statusCode int, errorMessage string) {
	statusCode = 400
	errorMessage = ""

	messager, ok := err.(ErrorMessager)
	if ok {
		errorMessage = messager.ErrorMessage()
	}

	coder, ok := err.(ErrorStatusCoder)
	if ok {
		statusCode = coder.ErrorStatusCode()
	}

	return statusCode, errorMessage
}

type HttpMessageError struct {
	statusCode int
	err        error
}

func NewHttpMessageError(statusCode int, err error) *HttpMessageError {
	return &HttpMessageError{
		statusCode: statusCode,
		err:        err,
	}
}

func (h *HttpMessageError) ErrorMessage() string {
	return h.err.Error()
}

func (h *HttpMessageError) ErrorStatusCode() int {
	return h.statusCode
}

func (h *HttpMessageError) Error() string {
	return h.err.Error()
}
