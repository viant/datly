package handler

import (
	"context"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/xdatly/handler/validator"
)

type Validator struct {
	validator *expand.Validator
}

func (v *Validator) Validate(ctx context.Context, any interface{}, options ...validator.Option) (*validator.Validation, error) {
	opt := &validator.Options{}
	for _, option := range options {
		option(opt)
	}

	var opts []interface{}
	validate, err := v.validator.Validate(any, opts...)
	if err != nil {
		return nil, err
	}

	result := &validator.Validation{
		Violations: make([]*validator.Violation, 0, len(validate.Violations)),
		Failed:     validate.Failed,
	}

	for _, violation := range validate.Violations {
		result.Violations = append(result.Violations, &validator.Violation{
			Location: violation.Location,
			Field:    violation.Field,
			Value:    violation.Value,
			Message:  violation.Message,
			Check:    violation.Check,
		})
	}

	return result, nil
}
