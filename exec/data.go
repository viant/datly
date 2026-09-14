package exec

import (
	"context"

	xhandler "github.com/viant/xdatly/handler"
)

// DataSource opens one lazy handler data capability for an invocation.
type DataSource interface {
	Open(ctx context.Context) (xhandler.Data, error)
}
