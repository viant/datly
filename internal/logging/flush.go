package logging

import (
	"io"
	"net/http"
)

type flushErrorWriter struct {
	http.ResponseWriter
	flush func() error
}

func (w *flushErrorWriter) FlushError() error           { return w.flush() }
func (w *flushErrorWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// PreserveFlushError adds only the FlushError capability present on source.
// Each combination conserves httpsnoop's five optional writer interfaces.
func PreserveFlushError(w http.ResponseWriter, source http.ResponseWriter, flush func() error) http.ResponseWriter {
	if _, ok := source.(interface{ FlushError() error }); !ok {
		return w
	}
	base := &flushErrorWriter{ResponseWriter: w, flush: flush}
	mask := 0
	flusher, ok0 := w.(http.Flusher)
	if ok0 {
		mask |= 1
	}
	hijacker, ok1 := w.(http.Hijacker)
	if ok1 {
		mask |= 2
	}
	pusher, ok2 := w.(http.Pusher)
	if ok2 {
		mask |= 4
	}
	notifier, ok3 := w.(http.CloseNotifier)
	if ok3 {
		mask |= 8
	}
	reader, ok4 := w.(io.ReaderFrom)
	if ok4 {
		mask |= 16
	}
	switch mask {
	case 0:
		return struct{ *flushErrorWriter }{base}
	case 1:
		return struct {
			*flushErrorWriter
			http.Flusher
		}{base, flusher}
	case 2:
		return struct {
			*flushErrorWriter
			http.Hijacker
		}{base, hijacker}
	case 3:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Hijacker
		}{base, flusher, hijacker}
	case 4:
		return struct {
			*flushErrorWriter
			http.Pusher
		}{base, pusher}
	case 5:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Pusher
		}{base, flusher, pusher}
	case 6:
		return struct {
			*flushErrorWriter
			http.Hijacker
			http.Pusher
		}{base, hijacker, pusher}
	case 7:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Hijacker
			http.Pusher
		}{base, flusher, hijacker, pusher}
	case 8:
		return struct {
			*flushErrorWriter
			http.CloseNotifier
		}{base, notifier}
	case 9:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.CloseNotifier
		}{base, flusher, notifier}
	case 10:
		return struct {
			*flushErrorWriter
			http.Hijacker
			http.CloseNotifier
		}{base, hijacker, notifier}
	case 11:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Hijacker
			http.CloseNotifier
		}{base, flusher, hijacker, notifier}
	case 12:
		return struct {
			*flushErrorWriter
			http.Pusher
			http.CloseNotifier
		}{base, pusher, notifier}
	case 13:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Pusher
			http.CloseNotifier
		}{base, flusher, pusher, notifier}
	case 14:
		return struct {
			*flushErrorWriter
			http.Hijacker
			http.Pusher
			http.CloseNotifier
		}{base, hijacker, pusher, notifier}
	case 15:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Hijacker
			http.Pusher
			http.CloseNotifier
		}{base, flusher, hijacker, pusher, notifier}
	case 16:
		return struct {
			*flushErrorWriter
			io.ReaderFrom
		}{base, reader}
	case 17:
		return struct {
			*flushErrorWriter
			http.Flusher
			io.ReaderFrom
		}{base, flusher, reader}
	case 18:
		return struct {
			*flushErrorWriter
			http.Hijacker
			io.ReaderFrom
		}{base, hijacker, reader}
	case 19:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Hijacker
			io.ReaderFrom
		}{base, flusher, hijacker, reader}
	case 20:
		return struct {
			*flushErrorWriter
			http.Pusher
			io.ReaderFrom
		}{base, pusher, reader}
	case 21:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Pusher
			io.ReaderFrom
		}{base, flusher, pusher, reader}
	case 22:
		return struct {
			*flushErrorWriter
			http.Hijacker
			http.Pusher
			io.ReaderFrom
		}{base, hijacker, pusher, reader}
	case 23:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Hijacker
			http.Pusher
			io.ReaderFrom
		}{base, flusher, hijacker, pusher, reader}
	case 24:
		return struct {
			*flushErrorWriter
			http.CloseNotifier
			io.ReaderFrom
		}{base, notifier, reader}
	case 25:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.CloseNotifier
			io.ReaderFrom
		}{base, flusher, notifier, reader}
	case 26:
		return struct {
			*flushErrorWriter
			http.Hijacker
			http.CloseNotifier
			io.ReaderFrom
		}{base, hijacker, notifier, reader}
	case 27:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Hijacker
			http.CloseNotifier
			io.ReaderFrom
		}{base, flusher, hijacker, notifier, reader}
	case 28:
		return struct {
			*flushErrorWriter
			http.Pusher
			http.CloseNotifier
			io.ReaderFrom
		}{base, pusher, notifier, reader}
	case 29:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Pusher
			http.CloseNotifier
			io.ReaderFrom
		}{base, flusher, pusher, notifier, reader}
	case 30:
		return struct {
			*flushErrorWriter
			http.Hijacker
			http.Pusher
			http.CloseNotifier
			io.ReaderFrom
		}{base, hijacker, pusher, notifier, reader}
	case 31:
		return struct {
			*flushErrorWriter
			http.Flusher
			http.Hijacker
			http.Pusher
			http.CloseNotifier
			io.ReaderFrom
		}{base, flusher, hijacker, pusher, notifier, reader}
	}
	panic("unreachable writer capability combination")
}
