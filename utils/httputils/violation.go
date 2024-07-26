package httputils

import (
	"fmt"
	"github.com/viant/govalidator"
	sqlxvalidator "github.com/viant/sqlx/io/validator"
	"github.com/viant/xdatly/handler/validator"
)

type Violations []*validator.Violation

func (v Violations) MergeGoViolation(violations []*govalidator.Violation) validator.Violations {
	if len(violations) == 0 {
		return validator.Violations{}
	}
	var ret []*validator.Violation
	for _, item := range v {
		ret = append(ret, item)
	}
	for _, violation := range violations {
		aViolation := validator.Violation(*violation)
		ret = append(ret, &aViolation)
	}
	return ret
}

func (v Violations) MergeSqlViolation(violations []*sqlxvalidator.Violation) validator.Violations {
	if len(violations) == 0 {
		return validator.Violations{}
	}
	var ret []*validator.Violation
	for _, item := range v {
		ret = append(ret, item)
	}
	for _, violation := range violations {
		aViolation := validator.Violation(*violation)
		ret = append(ret, &aViolation)
	}
	return ret
}

func (v Violations) MergeErrors(errors []*Error) validator.Violations {
	if len(errors) == 0 {
		return validator.Violations{}
	}
	var ret []*validator.Violation

	for _, anError := range errors {
		aViolation := &validator.Violation{
			Location: anError.View + "/" + anError.Parameter,
			Value:    anError.Object,
			Check:    fmt.Sprint("%T", anError.Error()),
			Message:  anError.Message,
		}
		ret = append(ret, aViolation)
	}
	return ret
}

func (v Violations) MergeCustom(custom map[string]interface{}) validator.Violations {
	var result []*validator.Violation
	for k, v := range custom {
		violation := &validator.Violation{Location: k}
		result = append(result, violation)
		if err, ok := v.(error); ok {
			violation.Message = err.Error()
			violation.Check = fmt.Sprintf("%T", err)
			continue
		}
		violation.Value = v
	}
	return result
}
