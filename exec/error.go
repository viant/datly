package exec

import (
	"errors"
	"net/http"

	"github.com/viant/bindly"
)

// ErrorMessage projects errors without an explicit response.BodyError payload.
// Authored binding messages remain intentional policy, including ${error};
// unclassified server failures never acquire a public diagnostic message.
func ErrorMessage(err error, status int) string {
	if err == nil {
		return http.StatusText(status)
	}
	var binding *bindly.BindingError
	if errors.As(err, &binding) && binding.Message != "" {
		return binding.Error()
	}
	if status < http.StatusInternalServerError {
		return err.Error()
	}
	return http.StatusText(status)
}
