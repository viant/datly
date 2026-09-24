package exec

import (
	"context"

	xhandler "github.com/viant/xdatly/handler"
)

// DataSource opens one lazy handler data capability for an invocation.
type DataSource interface {
	Open(ctx context.Context) (xhandler.Data, error)
}

// ConnectorDataSourceProvider supplies a lazy source for an explicitly named
// connector. The SQL layer owns source construction; the invocation engine
// owns its transaction lifecycle. Unknown names must not select a default.
type ConnectorDataSourceProvider interface {
	ConnectorDataSource(ctx context.Context, name string) (DataSource, error)
}
