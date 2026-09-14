package http

import (
	"io"
	stdhttp "net/http"

	"github.com/felixge/httpsnoop"
)

// corsResponseWriter applies configured policy at header commitment, after any
// application response headers, and preserves Origin variance for cached errors.
type corsResponseWriter struct {
	stdhttp.ResponseWriter
	policy  *corsPolicy
	request *stdhttp.Request
	method  string
	written bool
}

func (w *corsResponseWriter) Unwrap() stdhttp.ResponseWriter { return w.ResponseWriter }
func (w *corsResponseWriter) WriteHeader(status int) {
	if w.written {
		return
	}
	if status >= 100 && status < 200 && status != stdhttp.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	w.written = true
	for _, name := range []string{"Access-Control-Allow-Origin", "Access-Control-Allow-Methods", "Access-Control-Allow-Headers", "Access-Control-Expose-Headers", "Access-Control-Allow-Credentials", "Access-Control-Max-Age"} {
		w.Header().Del(name)
	}
	w.policy.apply(w.ResponseWriter, w.request, w.method, false)
	w.ResponseWriter.WriteHeader(status)
}
func (w *corsResponseWriter) Write(data []byte) (int, error) {
	if !w.written {
		w.WriteHeader(stdhttp.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

// wrap delegates optional-interface preservation to the existing native wrapper.
// Each hook commits CORS before a supported operation can implicitly send headers.
func (w *corsResponseWriter) wrap() stdhttp.ResponseWriter {
	return httpsnoop.Wrap(w.ResponseWriter, httpsnoop.Hooks{
		WriteHeader: func(httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc { return w.WriteHeader },
		Write:       func(httpsnoop.WriteFunc) httpsnoop.WriteFunc { return w.Write },
		Flush: func(next httpsnoop.FlushFunc) httpsnoop.FlushFunc {
			return func() {
				if !w.written {
					w.WriteHeader(stdhttp.StatusOK)
				}
				next()
			}
		},
		ReadFrom: func(next httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				if !w.written {
					w.WriteHeader(stdhttp.StatusOK)
				}
				return next(src)
			}
		},
	})
}
