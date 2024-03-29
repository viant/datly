package expand

import (
	"context"
	"github.com/viant/govalidator"
	"runtime/debug"
	"strconv"
)

var goValidator = NewValidator()

func CommonValidator() *Validator {
	return goValidator
}

type Validator struct {
	Service *govalidator.Service
}

func (c *Validator) WithPresence() govalidator.Option {
	return govalidator.WithSetMarker()
}

func (c *Validator) WithLocation(loc string) govalidator.Option {
	pathKind := govalidator.PathKindRoot
	if _, err := strconv.Atoi(loc); err == nil {
		pathKind = govalidator.PathKindIndex
	}
	return govalidator.WithPath(&govalidator.Path{Name: loc, Kind: pathKind})
}

//Validate validates destination
func (c *Validator) Validate(dest interface{}, opts ...interface{}) (*govalidator.Validation, error) {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			panic(r)
		}
	}()

	var options []govalidator.Option
	for _, opt := range opts {
		if o, ok := opt.(govalidator.Option); ok {
			options = append(options, o)
		}
	}
	return c.Service.Validate(context.Background(), dest, options...)
}

//NewValidator creates a validator
func NewValidator() *Validator {
	return &Validator{Service: govalidator.New()}
}
