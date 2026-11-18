package readerlookup

import (
	"errors"
	"net/http"
	"reflect"

	"github.com/viant/xdatly/response"
)

// classification carries a native lookup result, not an authored response body.
// Presentation can therefore retain the component's declared output contract.
type classification struct{ code int }

var joinType = reflect.TypeOf(errors.Join(errors.New("lookup join type")))

func (e *classification) Error() string   { return http.StatusText(e.code) }
func (e *classification) StatusCode() int { return e.code }

func EmptyResult(exists bool) error {
	code := http.StatusNotFound
	if exists {
		code = http.StatusForbidden
	}
	return &classification{code: code}
}

// Fallback preserves the original minimal body only after presentation has
// declined a declared envelope. Only the native error and standard-library
// joins repeating that identical error retain classification authority.
func Fallback(err error, status int) (response.Status, bool) {
	if status != http.StatusForbidden && status != http.StatusNotFound {
		return response.Status{}, false
	}
	budget := 64
	var selected *classification
	var visit func(error) bool
	visit = func(err error) bool {
		budget--
		if err == nil || budget < 0 {
			return false
		}
		if classified, ok := err.(*classification); ok {
			if classified == nil || classified.code != status || (selected != nil && selected != classified) {
				return false
			}
			selected = classified
			return true
		}
		if reflect.TypeOf(err) != joinType {
			return false
		}
		children := err.(interface{ Unwrap() []error }).Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !visit(child) {
				return false
			}
		}
		return true
	}
	if !visit(err) || selected == nil {
		return response.Status{}, false
	}
	return response.Status{Status: "error", Message: http.StatusText(status)}, true
}
