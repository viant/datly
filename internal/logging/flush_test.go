package logging

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

type allWriterCapabilities struct{ http.ResponseWriter }

func (*allWriterCapabilities) Flush()                                       {}
func (*allWriterCapabilities) Hijack() (net.Conn, *bufio.ReadWriter, error) { return nil, nil, nil }
func (*allWriterCapabilities) Push(string, *http.PushOptions) error         { return nil }
func (*allWriterCapabilities) CloseNotify() <-chan bool                     { return nil }
func (*allWriterCapabilities) ReadFrom(io.Reader) (int64, error)            { return 0, nil }
func (*allWriterCapabilities) FlushError() error                            { return nil }
func TestFlushErrorConservesEveryCapabilityCombination(t *testing.T) {
	rich := &allWriterCapabilities{ResponseWriter: httptest.NewRecorder()}
	writers := []http.ResponseWriter{
		struct{ http.ResponseWriter }{rich},
		struct {
			http.ResponseWriter
			http.Flusher
		}{rich, rich},
		struct {
			http.ResponseWriter
			http.Hijacker
		}{rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Hijacker
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Pusher
		}{rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Pusher
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Hijacker
			http.Pusher
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Hijacker
			http.Pusher
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.CloseNotifier
		}{rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.CloseNotifier
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Hijacker
			http.CloseNotifier
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Pusher
			http.CloseNotifier
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Pusher
			http.CloseNotifier
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Hijacker
			http.Pusher
			http.CloseNotifier
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Hijacker
			http.Pusher
			http.CloseNotifier
		}{rich, rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			io.ReaderFrom
		}{rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			io.ReaderFrom
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Hijacker
			io.ReaderFrom
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Hijacker
			io.ReaderFrom
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Pusher
			io.ReaderFrom
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Pusher
			io.ReaderFrom
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Hijacker
			http.Pusher
			io.ReaderFrom
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Hijacker
			http.Pusher
			io.ReaderFrom
		}{rich, rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.CloseNotifier
			io.ReaderFrom
		}{rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.CloseNotifier
			io.ReaderFrom
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Hijacker
			http.CloseNotifier
			io.ReaderFrom
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Hijacker
			http.CloseNotifier
			io.ReaderFrom
		}{rich, rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Pusher
			http.CloseNotifier
			io.ReaderFrom
		}{rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Pusher
			http.CloseNotifier
			io.ReaderFrom
		}{rich, rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Hijacker
			http.Pusher
			http.CloseNotifier
			io.ReaderFrom
		}{rich, rich, rich, rich, rich},
		struct {
			http.ResponseWriter
			http.Flusher
			http.Hijacker
			http.Pusher
			http.CloseNotifier
			io.ReaderFrom
		}{rich, rich, rich, rich, rich, rich},
	}
	for mask, w := range writers {
		wrapped := PreserveFlushError(w, rich, rich.FlushError)
		if _, ok := wrapped.(http.Flusher); ok != (mask&1 != 0) {
			t.Errorf("mask %d: http.Flusher changed", mask)
		}
		if _, ok := wrapped.(http.Hijacker); ok != (mask&2 != 0) {
			t.Errorf("mask %d: http.Hijacker changed", mask)
		}
		if _, ok := wrapped.(http.Pusher); ok != (mask&4 != 0) {
			t.Errorf("mask %d: http.Pusher changed", mask)
		}
		if _, ok := wrapped.(http.CloseNotifier); ok != (mask&8 != 0) {
			t.Errorf("mask %d: http.CloseNotifier changed", mask)
		}
		if _, ok := wrapped.(io.ReaderFrom); ok != (mask&16 != 0) {
			t.Errorf("mask %d: io.ReaderFrom changed", mask)
		}
		if _, ok := wrapped.(interface{ FlushError() error }); !ok {
			t.Errorf("mask %d: lost FlushError", mask)
		}
	}
}
