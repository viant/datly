package status

import (
	"net/http"

	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/utils/types"
	"github.com/viant/govalidator"
	svalidator "github.com/viant/sqlx/io/validator"
	"github.com/viant/xdatly/handler/response"
)

func NormalizeErr(err error, statusCode int) (int, string, interface{}) {
	violations := httputils.Violations{}
	switch actual := err.(type) {
	case *response.Error:
		code := actual.StatusCode()
		if code == 0 {
			code = http.StatusInternalServerError
		}
		// For explicit 4xx we trust the message, for 5xx we keep it generic.
		if code >= http.StatusInternalServerError {
			return code, http.StatusText(http.StatusInternalServerError), nil
		}
		return code, actual.Message, nil
	case *svalidator.Validation:
		ret := violations.MergeSqlViolation(actual.Violations)
		return http.StatusBadRequest, err.Error(), ret
	case *govalidator.Validation:
		ret := violations.MergeGoViolation(actual.Violations)
		return http.StatusBadRequest, actual.Error(), ret
	case *response.Errors:
		// Treat aggregated errors as validation-like by default.
		actual.SetStatusCode(http.StatusBadRequest)
		for _, anError := range actual.Errors {
			isObj := types.IsObject(anError.Err)
			if isObj {
				statusCode, anError.Message, anError.Object = NormalizeErr(anError.Err, http.StatusBadRequest)
			} else {
				statusCode, anError.Message, anError.Object = NormalizeErr(anError.Err, http.StatusBadRequest)
			}
			if statusCode > actual.StatusCode() {
				actual.SetStatusCode(statusCode)
			}
		}
		return actual.StatusCode(), actual.Message, actual.Errors
	case *expand.ErrorResponse:
		if actual.StatusCode != 0 {
			statusCode = actual.StatusCode
		}
		// If no status code was set on the error response, treat it as a client error.
		if statusCode == 0 {
			statusCode = http.StatusBadRequest
		}
		return statusCode, actual.Message, actual.Content
	default:
		// Any non-validation error is treated as an internal server error with a generic message.
		// The full error (including DB/sqlx failures) is still available in logs via exec.Context.SetError(err).
		return http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError), nil
	}
}
