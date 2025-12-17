package status

import (
	"net/http"
	"strings"

	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/utils/httputils"
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
			code = statusCode
		}
		if code == 0 {
			code = http.StatusBadRequest
		}
		// For explicit 5xx we keep response generic, for 4xx we trust the configured message.
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
		// Respect existing status/message set on aggregated errors (often parameter-driven).
		if actual.StatusCode() == 0 {
			if statusCode == 0 {
				actual.SetStatusCode(http.StatusBadRequest)
			} else {
				actual.SetStatusCode(statusCode)
			}
		}
		if actual.Message == "" && len(actual.Errors) > 0 {
			actual.Message = actual.Errors[0].Message
		}

		for _, anError := range actual.Errors {
			code := anError.StatusCode()

			switch {
			case code >= http.StatusInternalServerError:
				// Explicitly marked as server error at parameter level: generic message.
				anError.Message = http.StatusText(http.StatusInternalServerError)
			case code == 0:
				// No explicit status on this parameter error; classify underlying cause.
				innerStatus, innerMsg, innerObj := NormalizeErr(anError.Err, actual.StatusCode())
				anError.Code = innerStatus
				if innerMsg != "" {
					anError.Message = innerMsg
				}
				if innerObj != nil {
					anError.Object = innerObj
				}
				code = innerStatus
			default:
				// 4xx with configured status/message – leave as defined on the parameter.
			}

			if code > actual.StatusCode() {
				actual.SetStatusCode(code)
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
		// Only DB-caused errors are mapped to 500 with a generic message.
		if isDatabaseError(err) {
			return http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError), nil
		}
		if statusCode == 0 {
			statusCode = http.StatusBadRequest
		}
		return statusCode, err.Error(), nil
	}
}

// isDatabaseError detects errors that originate from DB/sqlx execution.
// These are the only errors that should be remapped to 500 with a generic message.
func isDatabaseError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if msg == "" {
		return false
	}

	// Known DB-related error patterns from reader/executor paths.
	if strings.Contains(msg, "database error occured while fetching Data") {
		return true
	}
	if strings.Contains(msg, "error occured while connecting to database") {
		return true
	}
	if strings.Contains(msg, "failed to get db:") {
		return true
	}

	return false
}
