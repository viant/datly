package expand

import (
	"fmt"
)

type ResponseBuilder struct {
	Content      map[string]interface{} `velty:"-"`
	ResponseCode int                    `velty:"-"`
}

type EmbeddableMap map[string]interface{}

type ErrorResponse struct {
	Content    EmbeddableMap
	Message    string
	StatusCode int
}

func (h *ErrorResponse) ErrorMessage() string {
	return h.Message
}

func (h *ErrorResponse) ErrorStatusCode() int {
	return h.StatusCode
}

func (h *ErrorResponse) Error() string {
	return h.Message
}

func (b *ResponseBuilder) Failf(format string, args ...interface{}) (string, error) {
	var statusCode int
	statusCode, args = b.extractStatusCode(args)
	return b.FailfWithStatusCode(statusCode, format, args...)
}

func (b *ResponseBuilder) CallbackFailf(format string, args ...interface{}) Callback {
	copiedArgs := copyValues(args)
	return func(_ interface{}, _ error) error {
		_, err := b.Failf(format, copiedArgs...)
		return err
	}
}

func (b *ResponseBuilder) FailfWithStatusCode(statusCode int, format string, args ...interface{}) (string, error) {
	args = dereferencer.derefArgs(args...)
	return "", &ErrorResponse{
		Content:    b.Content,
		Message:    fmt.Sprintf(format, args...),
		StatusCode: statusCode,
	}
}

func (b *ResponseBuilder) CallbackFailfWithStatusCode(statusCode int, format string, args ...interface{}) Callback {
	copiedArgs := copyValues(args)
	return func(object interface{}, err error) error {
		_, err = b.FailfWithStatusCode(statusCode, format, copiedArgs...)
		return err
	}
}

func (b *ResponseBuilder) Add(key string, value interface{}) string {
	return b.Put(key, value)
}

func (b *ResponseBuilder) CallbackAdd(key string, value interface{}) Callback {
	arg := copyValue(value)
	return func(object interface{}, err error) error {
		b.Add(key, arg)
		return nil
	}
}

func (b *ResponseBuilder) StatusCode(statusCode int) string {
	b.ResponseCode = statusCode
	return ""
}

func (b *ResponseBuilder) CallbackStatusCode(statusCode int) Callback {
	return func(object interface{}, err error) error {
		b.StatusCode(statusCode)
		return nil
	}
}

func (b *ResponseBuilder) Put(key string, value interface{}) string {
	coppied := copyValue(value)
	b.Content[key] = coppied
	return ""
}

func (b *ResponseBuilder) CallbackPut(key string, value interface{}) Callback {
	arg := copyValue(value)
	return func(object interface{}, err error) error {
		b.Put(key, arg)
		return nil
	}
}

func (b *ResponseBuilder) extractStatusCode(args []interface{}) (int, []interface{}) {
	for i, arg := range args {
		switch actual := arg.(type) {
		case StatusCode:
			statusCode := int(actual)
			if len(args) == 1 {
				return statusCode, nil
			}

			switch i {
			case len(args) - 1:
				return statusCode, args[:len(args)-1]
			case 0:
				return statusCode, args[1:]
			default:
				copy(args[i-1:], args[i+1:])
				return statusCode, args[:len(args)-1]
			}
		}
	}

	return b.ResponseCode, args
}
