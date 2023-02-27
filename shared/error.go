package shared

type ResponseError struct {
	StatusCode int
	Origin     error
}

func (e *ResponseError) Error() string {
	return e.Origin.Error()
}
