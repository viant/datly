package http

import (
	"context"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xexec "github.com/viant/xdatly/exec"
	xresponse "github.com/viant/xdatly/response"
)

type failingHTTPWriter struct{ header stdhttp.Header }

func (w *failingHTTPWriter) Header() stdhttp.Header  { return w.header }
func (*failingHTTPWriter) WriteHeader(int)           {}
func (*failingHTTPWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestHTTPLoggingCommittedBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		serve  stdhttp.HandlerFunc
	}{
		{"early", 403, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { w.WriteHeader(403) }},
		{"head", 204, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { w.WriteHeader(204) }},
		{"informational", 202, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			w.WriteHeader(103)
			w.WriteHeader(202)
			w.WriteHeader(500)
		}},
		{"implicit", 200, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = w.Write([]byte("ok")) }},
		{"runtime completion", 201, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			xexec.GetContext(r.Context()).Complete(time.Now().Add(-time.Second), nil)
			w.WriteHeader(201)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count := 0
			serveHTTPObserved(httptest.NewRecorder(), httptest.NewRequest("HEAD", "/", nil), "v", tc.serve, func(ctx context.Context, e *xexec.Context) {
				count++
				if e.StatusCode != tc.status {
					t.Errorf("status %d", e.StatusCode)
				}
				if e.ElapsedMs < 0 {
					t.Errorf("stale runtime timing %d", e.ElapsedMs)
				}
			})
			if count != 1 {
				t.Fatalf("completion count %d", count)
			}
		})
	}
}

func TestHTTPLoggingErrorsAndCapabilities(t *testing.T) {
	writer := &failingHTTPWriter{header: stdhttp.Header{}}
	serveHTTPObserved(writer, httptest.NewRequest("GET", "/", nil), "v", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if _, ok := w.(stdhttp.Flusher); ok {
			t.Fatal("invented Flusher")
		}
		if _, ok := w.(io.ReaderFrom); ok {
			t.Fatal("invented ReaderFrom")
		}
		_, _ = w.Write([]byte("body"))
		w.WriteHeader(500)
	}, func(_ context.Context, e *xexec.Context) {
		if e.StatusCode != 200 || e.Error != "write failed" {
			t.Fatalf("status/error %d %q", e.StatusCode, e.Error)
		}
	})
	serveHTTPObserved(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "v", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if _, ok := w.(stdhttp.Flusher); !ok {
			t.Fatal("lost Flusher")
		}
		recordHTTPError(r.Context(), errors.New("encoding failed"))
		w.WriteHeader(500)
	}, func(_ context.Context, e *xexec.Context) {
		if e.Error != "encoding failed" {
			t.Fatal(e.Error)
		}
	})
}

func TestHTTPLoggingPreservesApplicationPanic(t *testing.T) {
	marker := &struct{}{}
	completed := false
	defer func() {
		if recover() != marker {
			t.Error("application panic replaced")
		}
		if !completed {
			t.Error("missing completion")
		}
	}()
	serveHTTPObserved(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "v", func(stdhttp.ResponseWriter, *stdhttp.Request) { panic(marker) }, func(_ context.Context, e *xexec.Context) { completed = true; panic("logging panic") })
}

func TestHTTPLoggingKeepsWriteFailureAfterExecutionFailure(t *testing.T) {
	writer := &failingHTTPWriter{header: stdhttp.Header{}}
	serveHTTPObserved(writer, httptest.NewRequest("GET", "/", nil), "v", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		recordHTTPError(r.Context(), errors.New("execution failed"))
		w.WriteHeader(403)
		_, _ = w.Write([]byte("error body"))
	}, func(_ context.Context, e *xexec.Context) {
		if e.StatusCode != 403 || !strings.Contains(e.Error, "execution failed") || !strings.Contains(e.Error, "write failed") {
			t.Fatal("later write error lost")
		}
	})
}

type hostileLoggingError struct{}

func (hostileLoggingError) Error() string { panic("private formatter failure") }
func (hostileLoggingError) Is(error) bool { panic("private classification failure") }

func TestHTTPLoggingHostileErrorCannotReplaceApplicationPanic(t *testing.T) {
	marker := &struct{}{}
	defer func() {
		if recover() != marker {
			t.Fatal("logging replaced application panic")
		}
	}()
	serveHTTPObserved(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "v", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		recordHTTPError(r.Context(), hostileLoggingError{})
		recordHTTPError(r.Context(), errors.New("second failure"))
		panic(marker)
	}, func(context.Context, *xexec.Context) {})
}

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("source read failed") }
func (failingBody) WriteTo(w io.Writer) (int64, error) {
	return 0, errors.New("source write-to failed")
}

type failingBodyResponse struct{ *xresponse.Buffered }

func (failingBodyResponse) Body() io.Reader { return failingBody{} }
func TestHTTPLoggingCapturesBodyCopyError(t *testing.T) {
	serveHTTPObserved(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "v", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		writeResponse(w, 200, true, failingBodyResponse{xresponse.NewBuffered()}, r)
	}, func(_ context.Context, e *xexec.Context) {
		if e.StatusCode != 200 || !strings.Contains(e.Error, "source write-to failed") {
			t.Fatalf("body-copy evidence lost: status=%d error=%s", e.StatusCode, e.Error)
		}
	})
}
