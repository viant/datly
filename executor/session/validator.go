package session

import (
	"context"
	"github.com/viant/govalidator"
	"github.com/viant/xdatly/handler/validator"
)

var goValidator = govalidator.New()

type Validator struct{}

func (v *Validator) Validate(ctx context.Context, any interface{}, opts ...validator.Option) *validator.Validation {
	options := &validator.Options{}
	options.Apply(opts)
	var gOptions []govalidator.Option
	validation := &validator.Validation{Violations: make([]*validator.Violation, 0)}
	if options.WithShallow {
		gOptions = append(gOptions, govalidator.WithShallow(true))
	}
	if options.WithSetMarker {
		gOptions = append(gOptions, govalidator.WithSetMarker())
	}

	if options.WithValidation != nil && len(options.WithValidation.Violations) > 0 {
		validation.Violations = append(validation.Violations, options.WithValidation.Violations...)
		validation.Failed = len(validation.Violations) > 0
	}
	ret, err := goValidator.Validate(ctx, any, gOptions...)
	if err != nil {
		validation.Append("/", "", "", "error", err.Error())
	}
	validation.Violations = Violations(validation.Violations).Merge(ret.Violations)
	return validation
}

type Violations []*validator.Violation

func (v Violations) Merge(violations []*govalidator.Violation) Violations {
	if len(violations) == 0 {
		return v
	}
	for _, violation := range violations {
		aViolation := validator.Violation(*violation)
		v = append(v, &aViolation)
	}
	return v
}
