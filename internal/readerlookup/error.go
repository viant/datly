package readerlookup

import (
	"net/http"

	"github.com/viant/xdatly/response"
)

// classification carries a native lookup result, not an authored response body.
// Presentation can therefore retain the component's declared output contract.
type classification struct{ code int }

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
// declined a declared envelope. Wrappers and joined errors do not grant this
// authority, nor does an error that disagrees with the effective status.
func Fallback(err error, status int) (response.Status, bool) {
	classified, ok := err.(*classification)
	if !ok || classified == nil || classified.code != status ||
		(status != http.StatusForbidden && status != http.StatusNotFound) {
		return response.Status{}, false
	}
	return response.Status{Status: "error", Message: http.StatusText(status)}, true
}
