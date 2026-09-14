package engine

import (
	"context"
	"fmt"
	"github.com/viant/bindly/locator"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

type frameworkValidationData interface{ FrameworkValidator() xhandler.Validator }

func (s *dataScope) frameworkValidatorProvider() locator.Provider {
	return handlerprovider.New(xhandler.FrameworkValidatorKey, func(ctx context.Context) (any, bool, error) {
		if s == nil {
			return nil, false, fmt.Errorf("framework validation requires an invocation data source")
		}
		data, err := s.resolve(ctx)
		if err != nil {
			return nil, false, err
		}
		if (xshape.Runtime{}).IsNil(data) {
			return nil, false, fmt.Errorf("framework validation data is unavailable")
		}
		source, ok := data.(frameworkValidationData)
		if !ok {
			return nil, false, fmt.Errorf("data source does not supply framework validation")
		}
		validator := source.FrameworkValidator()
		if (xshape.Runtime{}).IsNil(validator) {
			return nil, false, fmt.Errorf("framework validator is unavailable")
		}
		return validator, true, nil
	})
}
