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
