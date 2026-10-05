package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
)

func capturedInitializationOutput(ctx context.Context, request Request, invocation rhandler.Invocation, cause error) (result any, failure error) {
	// finish has already begun; local recovery must let normal cleanup continue.
	defer func() {
		if value := recover(); value != nil {
			result = nil
			failure = dexec.NewPanicError("captured error output", value)
		}
	}()
	bridge, ok := request.Handler.(rhandler.CapturedErrorOutput)
	if !ok || request.OutputType == nil {
		return nil, nil
	}
	early, enabled := request.Handler.(rhandler.EarlyErrorOutputFinalizer)
	if !enabled || !early.EarlyErrorOutputEnabled() {
		return nil, nil
	}
	typed, ok := request.Handler.(rhandler.TypedHandler)
	if request.OutputType.Kind() != reflect.Struct || !ok || typed.OutputType() != request.OutputType {
		return nil, fmt.Errorf("captured error output conflicts with declared output contract")
	}
	result, failure = bridge.CapturedErrorOutput(ctx, invocation, cause)
	if result == nil {
		return nil, failure
	}
	value := reflect.ValueOf(result)
	if value.Type() != reflect.PointerTo(request.OutputType) || value.IsNil() {
		return nil, errors.Join(failure, fmt.Errorf("captured error output must be non-nil *%s", request.OutputType))
	}
	return result, failure
}
