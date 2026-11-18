package exec

import (
	"errors"
	"net/http"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/tagly/jsonx"
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
	if message, ok := WrapperValidationMessage(err, status); ok {
		return message
	}
	if status < http.StatusInternalServerError {
		return err.Error()
	}
	return http.StatusText(status)
}

// WrapperValidationMessage admits only native wrapper validation owned directly
// by an external body binding. Wrappers, joins and dependency chains must first
// pass the bounded output provenance projector; arbitrary diagnostics and
// implementations of PublicMessage do not establish public authority.
func WrapperValidationMessage(err error, status int) (string, bool) {
	if status != http.StatusBadRequest {
		return "", false
	}
	owner, ok := err.(*bindly.BindingError)
	if !ok || owner == nil || owner.Kind != "body" || owner.Name == "" || owner.Message != "" ||
		(owner.Code != 0 && owner.Code != http.StatusBadRequest) {
		return "", false
	}
	input, ok := owner.Unwrap().(*bindinput.Error)
	if !ok || input == nil || (input.Code != 0 && input.Code != http.StatusBadRequest) {
		return "", false
	}
	validation, ok := input.Cause.(*jsonx.WrapperError)
	if !ok {
		return "", false
	}
	return validation.PublicMessage()
}
