package httputils

import (
	"fmt"
	"github.com/viant/govalidator"
	sqlxvalidator "github.com/viant/sqlx/io/validator"
	"github.com/viant/xdatly/handler/validator"
)

type Violations []*validator.Violation

func (v Violations) MergeGoViolation(violations []*govalidator.Violation) Violations {
	if len(violations) == 0 {
		return v
	}
	for _, violation := range violations {
		aViolation := validator.Violation(*violation)
		v = append(v, &aViolation)
	}
	return v
}

func (v Violations) MergeSqlViolation(violations []*sqlxvalidator.Violation) []*validator.Violation {
	if len(violations) == 0 {
		return v
	}
	for _, violation := range violations {
		aViolation := validator.Violation(*violation)
		v = append(v, &aViolation)
	}
	return v
}

func (v Violations) MergeErrors(errors []*Error) []*validator.Violation {
	if len(errors) == 0 {
		return v
	}
	for _, anError := range errors {
		aViolation := &validator.Violation{
			Location: anError.View + "/" + anError.Parameter,
			Value:    anError.Object,
			Check:    fmt.Sprint("%T", anError.Error()),
			Message:  anError.Message,
		}
		v = append(v, aViolation)
	}
	return v
}

func (v Violations) MergeCustom(custom map[string]interface{}) []*validator.Violation {
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
