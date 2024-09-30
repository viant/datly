package status

import (
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
		return actual.StatusCode(), actual.Message, nil
	case *svalidator.Validation:
		ret := violations.MergeSqlViolation(actual.Violations)
		return statusCode, err.Error(), ret
	case *govalidator.Validation:
		ret := violations.MergeGoViolation(actual.Violations)
		return statusCode, actual.Error(), ret
	case *response.Errors:
		actual.SetStatusCode(statusCode)
		for _, anError := range actual.Errors {
			isObj := types.IsObject(anError.Err)
			if isObj {
				statusCode, anError.Message, anError.Object = NormalizeErr(anError.Err, statusCode)
			} else {
				statusCode, anError.Message, anError.Object = NormalizeErr(anError.Err, statusCode)
			}
		}
		actual.SetStatusCode(statusCode)
		return actual.StatusCode(), actual.Message, actual.Errors
	case *expand.ErrorResponse:
		if actual.StatusCode != 0 {
			statusCode = actual.StatusCode
		}
		return statusCode, actual.Message, actual.Content
	default:
		return statusCode, err.Error(), nil
	}
}
