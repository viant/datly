package status

import (
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/utils/types"
	"github.com/viant/govalidator"
	svalidator "github.com/viant/sqlx/io/validator"
)

func NormalizeErr(err error, statusCode int) (int, string, interface{}) {
	violations := httputils.Violations{}
	switch actual := err.(type) {
	case *httputils.HttpMessageError:
		return actual.ErrorStatusCode(), actual.ErrorMessage(), nil
	case *svalidator.Validation:
		violations = violations.MergeSqlViolation(actual.Violations)
		return statusCode, err.Error(), violations
	case *govalidator.Validation:
		violations = violations.MergeGoViolation(actual.Violations)
		return statusCode, actual.Error(), violations
	case *httputils.Errors:
		actual.SetStatus(statusCode)
		for _, anError := range actual.Errors {
			isObj := types.IsObject(anError.Err)
			if isObj {
				statusCode, anError.Message, anError.Object = NormalizeErr(anError.Err, statusCode)
			} else {
				statusCode, anError.Message, anError.Object = NormalizeErr(anError.Err, statusCode)
			}
		}

		actual.SetStatus(statusCode)

		return actual.ErrorStatusCode(), actual.Message, actual.Errors
	case *expand.ErrorResponse:
		if actual.StatusCode != 0 {
			statusCode = actual.StatusCode
		}

		return statusCode, actual.Message, actual.Content
	default:
		return statusCode, err.Error(), nil
	}
}
