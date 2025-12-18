package status

import (
	"net/http"

	"github.com/viant/datly/service/executor/expand"
	derrors "github.com/viant/datly/utils/errors"
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
		maxStatus := actual.StatusCode()
		if maxStatus == 0 {
			maxStatus = statusCode
		}
		if maxStatus == 0 {
			maxStatus = http.StatusBadRequest
		}
		hasServerError := maxStatus >= http.StatusInternalServerError

		for _, anError := range actual.Errors {
			code := anError.StatusCode()
			switch {
			case code >= http.StatusInternalServerError:
				anError.Message = http.StatusText(http.StatusInternalServerError)
				hasServerError = true
			case code == 0:
				innerStatus, innerMsg, innerObj := NormalizeErr(anError.Err, maxStatus)
				code = innerStatus
				anError.Code = innerStatus
				if innerMsg != "" {
					anError.Message = innerMsg
				}
				if innerObj != nil {
					anError.Object = innerObj
				}
				if code >= http.StatusInternalServerError {
					hasServerError = true
				}
			default:
				if code >= http.StatusInternalServerError {
					hasServerError = true
				}
			}

			if code > maxStatus {
				maxStatus = code
			}
		}

		if hasServerError {
			actual.Message = http.StatusText(http.StatusInternalServerError)
		} else if actual.Message == "" && len(actual.Errors) > 0 {
			actual.Message = actual.Errors[0].Message
		}

		if maxStatus == 0 {
			maxStatus = http.StatusBadRequest
		}

		return maxStatus, actual.Message, actual.Errors
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
		if derrors.IsDatabaseError(err) {
			return http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError), nil
		}
		if statusCode == 0 {
			statusCode = http.StatusBadRequest
		}
		return statusCode, err.Error(), nil
	}
}
