package converter

import "github.com/go-playground/validator"

var aValidator *validator.Validate

func init() {
	aValidator = validator.New()
}
