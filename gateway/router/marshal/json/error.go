package json

import (
	"fmt"
	"strings"
)

// Error represents a json unmarshal error
type Error struct {
	Path string
	Err  error
}

func (e *Error) Error() string {
	return fmt.Sprintf("failed to unmarshal %s, %v", e.Path, e.Err)
}

func NewError(path string, err error) *Error {
	if jErr, ok := err.(*Error); ok {
		if strings.HasPrefix(jErr.Path, "[") {
			path = path + jErr.Path
		} else {
			path = path + "." + jErr.Path
		}
		err = jErr.Err
	}
	return &Error{Path: path, Err: err}
}
