package runtime

import (
	"context"
	dexec "github.com/viant/datly/exec"
	xhandler "github.com/viant/xdatly/handler"
)

var _ xhandler.OutputSelection = (*scopedComponentInvoker)(nil)

func (*scopedComponentInvoker) CaptureChildOutputSelection(ctx context.Context) context.Context {
	return dexec.CaptureChildOutputSelection(ctx)
}
func (*scopedComponentInvoker) SelectedOutputFields(ctx context.Context, value any) xhandler.OutputFieldFilter {
	return dexec.SelectedOutputFields(ctx, value)
}
func (*scopedComponentInvoker) PublishOutputSelection(ctx context.Context, value any, filter xhandler.OutputFieldFilter) {
	dexec.PublishOutputSelection(ctx, value, filter)
}
