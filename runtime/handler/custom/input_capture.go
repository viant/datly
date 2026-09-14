package custom

import (
	"context"
	"fmt"
	"reflect"

	xhandler "github.com/viant/xdatly/handler"
)

func (h *contractHandler[I, O]) CaptureInput(ctx context.Context, value any) (any, error) {
	if h == nil || h.contract == nil {
		return nil, fmt.Errorf("custom handler contract is required")
	}
	capturer, ok := h.contract.(xhandler.InputCapturer[I])
	if !ok {
		return nil, nil
	}
	input, ok := value.(*I)
	if !ok || input == nil {
		return nil, fmt.Errorf("capture input must be *%s, got %T", reflect.TypeFor[I](), value)
	}
	return capturer.CaptureInput(ctx, input)
}
