package logging

import (
	"context"
	"errors"
	"io"
	stdhttp "net/http"
	"time"

	"github.com/felixge/httpsnoop"
	xexec "github.com/viant/xdatly/exec"
)

type httpObservationKey struct{}
type httpObservation struct {
	status int
	err    error
	errors int
}

func RecordHTTPError(ctx context.Context, err error) {
	if state, _ := ctx.Value(httpObservationKey{}).(*httpObservation); state != nil && err != nil && state.errors < 8 {
		state.err = errors.Join(state.err, err)
		state.errors++
	}
}

// ServeHTTPObserved owns completion after every gateway path and runtime finalizer.
// httpsnoop retains the exact optional interfaces of the underlying writer.
func ServeHTTPObserved(writer stdhttp.ResponseWriter, req *stdhttp.Request, version string, serve stdhttp.HandlerFunc, complete func(context.Context, *xexec.Context)) {
	state := &httpObservation{}
	execCtx := xexec.NewContext(req.Method, req.RequestURI, req.Header, version)
	ctx := context.WithValue(req.Context(), xexec.ContextKey, execCtx)
	ctx = WithIdentityObservation(context.WithValue(ctx, httpObservationKey{}, state))
	commit := func(status int) {
		if state.status == 0 && (status == stdhttp.StatusSwitchingProtocols || status >= 200) {
			state.status = status
		}
	}
	source := writer
	writer = httpsnoop.Wrap(writer, httpsnoop.Hooks{
		WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(status int) { next(status); commit(status) }
		},
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(data []byte) (int, error) {
				commit(stdhttp.StatusOK)
				n, err := next(data)
				RecordHTTPError(ctx, err)
				return n, err
			}
		},
		Flush: func(next httpsnoop.FlushFunc) httpsnoop.FlushFunc {
			return func() {
				commit(stdhttp.StatusOK)
				if flusher, ok := source.(interface{ FlushError() error }); ok {
					RecordHTTPError(ctx, flusher.FlushError())
					return
				}
				next()
			}
		},
		ReadFrom: func(next httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				commit(stdhttp.StatusOK)
				n, err := next(src)
				RecordHTTPError(ctx, err)
				return n, err
			}
		},
	})
	writer = PreserveFlushError(writer, source, func() error {
		commit(stdhttp.StatusOK)
		err := source.(interface{ FlushError() error }).FlushError()
		RecordHTTPError(ctx, err)
		return err
	})
	defer func() {
		applicationPanic := recover()
		func() {
			defer func() { _ = recover() }()
			if applicationPanic != nil {
				RecordHTTPError(ctx, errors.New("HTTP handler panicked"))
			}
			if state.status == 0 {
				state.status = stdhttp.StatusOK
				if applicationPanic != nil {
					state.status = stdhttp.StatusInternalServerError
				}
			}
			execCtx.StatusCode = state.status
			execCtx.SetError(state.err)
			completionErr := state.err
			if completionErr == nil && execCtx.Error != "" {
				completionErr = errors.New(execCtx.Error)
			}
			if completionErr == nil && state.status >= 400 {
				completionErr = errors.New(stdhttp.StatusText(state.status))
			}
			execCtx.Complete(time.Now(), completionErr)
			// Logging failures never replace an application panic or affect the response.
			complete(ctx, execCtx)
		}()
		if applicationPanic != nil {
			panic(applicationPanic)
		}
	}()
	serve(writer, req.WithContext(ctx))
}

// HTTPObserved identifies an existing outer boundary without exposing its state.
func HTTPObserved(ctx context.Context) bool {
	_, ok := ctx.Value(httpObservationKey{}).(*httpObservation)
	return ok
}
