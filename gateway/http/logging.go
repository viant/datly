package http

import (
	"context"
	"github.com/viant/datly/internal/logging"
	xexec "github.com/viant/xdatly/exec"
	"net/http"
)

func recordHTTPError(ctx context.Context, err error) { logging.RecordHTTPError(ctx, err) }
func serveHTTPObserved(w http.ResponseWriter, r *http.Request, version string, serve http.HandlerFunc, complete func(context.Context, *xexec.Context)) {
	logging.ServeHTTPObserved(w, r, version, serve, complete)
}
