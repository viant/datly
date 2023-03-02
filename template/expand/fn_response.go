package expand

import "fmt"

type ResponseBuilder struct {
	content map[string]interface{}
}

type EmbeddableMap map[string]interface{}

type HttpError struct {
	Content    EmbeddableMap
	Message    string
	StatusCode int
}

func (h *HttpError) Error() string {
	return h.Message
}

func (b *ResponseBuilder) Failf(format string, args ...interface{}) (string, error) {
	var statusCode int
	statusCode, args = b.extractStatusCode(args)
	return b.FailfWithStatusCode(statusCode, format, args...)
}

func (b *ResponseBuilder) FailfWithStatusCode(statusCode int, format string, args ...interface{}) (string, error) {
	return "", &HttpError{
		Content:    b.content,
		Message:    fmt.Sprintf(format, args...),
		StatusCode: statusCode,
	}
}

func (b *ResponseBuilder) Add(key string, value interface{}) string {
	coppied := copyValue(value)
	b.content[key] = coppied
	return ""
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

	return 0, args
}
