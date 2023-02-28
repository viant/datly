package converter

import "github.com/viant/govalidator"

var aValidator *govalidator.Service

func init() {
	aValidator = govalidator.New()
}
