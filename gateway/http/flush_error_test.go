package http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	xexec "github.com/viant/xdatly/exec"
)

type errorOnlyFlushWriter struct {
	http.ResponseWriter
	err     error
	flushes int
}

func (w *errorOnlyFlushWriter) FlushError() error { w.flushes++; return w.err }

type bothFlushWriter struct {
	*errorOnlyFlushWriter
	voidFlushes int
}

func (w *bothFlushWriter) Flush() { w.voidFlushes++ }

func TestObservedFlushError(t *testing.T) {
	for _, both := range []bool{false, true} {
		for _, cors := range []bool{false, true} {
			original := errors.New("flush failed")
			base := &errorOnlyFlushWriter{ResponseWriter: httptest.NewRecorder(), err: original}
			var writer http.ResponseWriter = base
			richer := &bothFlushWriter{errorOnlyFlushWriter: base}
			if both {
				writer = richer
			}
			serveHTTPObserved(writer, httptest.NewRequest("GET", "/", nil), "v", func(w http.ResponseWriter, r *http.Request) {
				if cors {
					w = (&corsResponseWriter{ResponseWriter: w, request: r, policy: &corsPolicy{}, method: "GET"}).wrap()
				}
				if _, ok := w.(http.Flusher); ok != both {
					t.Errorf("Flusher capability %v want %v", ok, both)
				}
				if _, ok := w.(io.ReaderFrom); ok {
					t.Error("invented ReaderFrom")
				}
				if _, ok := w.(http.Hijacker); ok {
					t.Error("invented Hijacker")
				}
				if _, ok := w.(http.Pusher); ok {
					t.Error("invented Pusher")
				}
				if _, ok := w.(http.CloseNotifier); ok {
					t.Error("invented CloseNotifier")
				}
				if err := http.NewResponseController(w).Flush(); err != original {
					t.Errorf("flush error %v", err)
				}
			}, func(_ context.Context, e *xexec.Context) {
				if e.StatusCode != 200 || e.Error == "" {
					t.Errorf("missing flush evidence %+v", e)
				}
			})
			if base.flushes != 1 || richer.voidFlushes != 0 {
				t.Errorf("duplicate or wrong flush: %d/%d", base.flushes, richer.voidFlushes)
			}
		}
	}
}

func TestObservedDirectFlushRecordsError(t *testing.T) {
	base := &errorOnlyFlushWriter{ResponseWriter: httptest.NewRecorder(), err: errors.New("flush failed")}
	writer := &bothFlushWriter{errorOnlyFlushWriter: base}
	serveHTTPObserved(writer, httptest.NewRequest("GET", "/", nil), "v", func(w http.ResponseWriter, r *http.Request) { w.(http.Flusher).Flush() }, func(_ context.Context, e *xexec.Context) {
		if e.Error == "" {
			t.Error("missing error")
		}
	})
	if base.flushes != 1 || writer.voidFlushes != 0 {
		t.Error("wrong flush")
	}
}
