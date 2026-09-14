package velty

import (
	"context"
	"fmt"

	rhandler "github.com/viant/datly/runtime/handler"
)

type Validator struct {
	ctx     context.Context
	service rhandler.ValidationService
}

func (v *Validator) Check(value interface{}) (string, error) {
	if v == nil || v.service == nil {
		return "", fmt.Errorf("velty validator capability is not configured")
	}
	result, err := v.service.Validate(v.ctx, value)
	if err != nil {
		return "", err
	}
	return "", result.Err()
}
