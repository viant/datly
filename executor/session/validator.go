package session

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/govalidator"
	sqlxvalidator "github.com/viant/sqlx/io/validator"
	"github.com/viant/xdatly/handler/validator"
)

var goValidator = govalidator.New()
var sqlxValidator = sqlxvalidator.New()

type SqlxValidator struct {
	validator Validator
	db        *sql.DB
}

func (v *SqlxValidator) Validate(ctx context.Context, any interface{}, opts ...validator.Option) (*validator.Validation, error) {
	options := &validator.Options{}
	options.Apply(opts)
	validation := getOrCreateValidation(options)
	err := v.validator.validateWithSqlx(ctx, any, validation, options)
	if err != nil {
		validation.Append("/", "", "", "error", err.Error())
	}
	return validation, nil
}

type Validator struct{}

func (v *Validator) Validate(ctx context.Context, any interface{}, opts ...validator.Option) (*validator.Validation, error) {
	options := &validator.Options{}
	options.Apply(opts)
	validation := getOrCreateValidation(options)
	err := v.validateWithGoValidator(ctx, any, validation, options)
	if err != nil {
		validation.Append("/", "", "", "error", err.Error())
	}
	if err = v.validateWithSqlx(ctx, any, validation, options); err != nil {
		validation.Append("/", "", "", "error", err.Error())
	}
	return validation, nil
}

func getOrCreateValidation(options *validator.Options) *validator.Validation {
	var validation *validator.Validation
	if options.WithValidation != nil {
		validation = options.WithValidation
	}
	if validation == nil {
		validation = &validator.Validation{Violations: make([]*validator.Violation, 0)}
	}
	return validation
}

func (v *Validator) validateWithGoValidator(ctx context.Context, any interface{}, validation *validator.Validation, options *validator.Options) error {
	var gOptions []govalidator.Option
	if options.WithShallow {
		gOptions = append(gOptions, govalidator.WithShallow(true))
	}
	if options.WithSetMarker {
		gOptions = append(gOptions, govalidator.WithSetMarker())
	}

	ret, err := goValidator.Validate(ctx, any, gOptions...)
	if ret != nil && len(ret.Violations) > 0 {
		validation.Violations = Violations(validation.Violations).MergeGoViolation(ret.Violations)
	}
	return err
}

func (v *Validator) validateWithSqlx(ctx context.Context, any interface{}, validation *validator.Validation, options *validator.Options) error {
	db := options.WithDB
	if db != nil {
		var sqlxOptions = []sqlxvalidator.Option{
			sqlxvalidator.WithUnique(true),
			sqlxvalidator.WithRef(true),
		}
		if options.WithSetMarker {
			sqlxOptions = append(sqlxOptions, sqlxvalidator.WithSetMarker())
		}
		ret, err := sqlxValidator.Validate(ctx, db, any, sqlxOptions...)
		if ret != nil && len(ret.Violations) > 0 {
			validation.Violations = Violations(validation.Violations).MergeSqlViolation(ret.Violations)
		}
		return err
	}
	return fmt.Errorf("%T was nil", db)
}

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

func NewValidator() *validator.Service {
	return validator.New(&Validator{})
}