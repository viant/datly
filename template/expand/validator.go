package expand

import (
	"context"
	"fmt"
	"github.com/viant/govalidator"
	"runtime/debug"
	"strconv"
)

var goValidator = NewValidator()

type Validator struct {
	*govalidator.Service
}

func (c *Validator) WithPresence() govalidator.Option {
	return govalidator.WithPresence()
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
	fmt.Printf("VV: %T %v\n", c, c)
	fmt.Printf("call %T %v %T %v %v\n", c.Service, c.Service, dest, dest, options)
	return c.Service.Validate(context.Background(), dest, options...)
}

//NewValidator creates a validator
func NewValidator() *Validator {
	return &Validator{Service: govalidator.New()}
}
