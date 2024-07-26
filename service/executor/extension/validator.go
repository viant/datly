package extension

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/utils/httputils"
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
	var err error
	if options.WithDB == nil {
		options.WithDB = v.db
	}
	if options.WithDB == nil {
		err = fmt.Errorf("db was empty")
	}
	if err == nil {
		err = v.validator.validateWithSqlx(ctx, any, validation, options)
	}
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
	if options.CanUseMarkerProvider != nil {
		gOptions = append(gOptions, govalidator.WithCanUseMarkerProvider(govalidator.CanUseMarkerProvider(options.CanUseMarkerProvider)))
	}
	if options.Location != "" {
		rootPath := govalidator.NewPath()
		gOptions = append(gOptions, govalidator.WithPath(rootPath.Field(options.Location)))
	}

	ret, err := goValidator.Validate(ctx, any, gOptions...)
	if ret != nil && len(ret.Violations) > 0 {
		validation.Violations = httputils.Violations(validation.Violations).MergeGoViolation(ret.Violations)
		validation.Failed = true
	}
	return err
}

func (v *Validator) validateWithSqlx(ctx context.Context, any interface{}, validation *validator.Validation, options *validator.Options) error {
	db := options.WithDB
	if db != nil {
		var sqlxOptions []sqlxvalidator.Option
		if options.WithUnique {
			sqlxOptions = append(sqlxOptions, sqlxvalidator.WithUnique(true))
		}
		if options.WithRef {
			sqlxOptions = append(sqlxOptions, sqlxvalidator.WithRef(true))
		}

		if len(sqlxOptions) == 0 {
			return nil
		}
		if options.WithShallow {
			sqlxOptions = append(sqlxOptions, sqlxvalidator.WithShallow(true))
		}
		if options.Location != "" {
			sqlxOptions = append(sqlxOptions, sqlxvalidator.WithLocation(options.Location))
		}
		sqlxOptions = append(sqlxOptions, sqlxvalidator.WithSetMarker())
		ret, err := sqlxValidator.Validate(ctx, db, any, sqlxOptions...)
		if ret != nil && len(ret.Violations) > 0 {
			validation.Violations = httputils.Violations(validation.Violations).MergeSqlViolation(ret.Violations)
			validation.Failed = true
		}
		return err
	}
	return nil
}

func NewValidator() *validator.Service {
	return validator.New(&Validator{})
}
