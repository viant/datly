package proxy

import (
	"bytes"
	"net/http"
)

//Writer represents http writer
type Writer struct {
	Code        int
	HeaderMap   http.Header
	Body        *bytes.Buffer
	wroteHeader bool
}

func (w *Writer) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.Code = code
	w.wroteHeader = true
}

func (w *Writer) Header() http.Header {
	return w.HeaderMap
}

func (w *Writer) Write(buf []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.Body.Write(buf)
}

func NewWriter() *Writer {
	return &Writer{
		HeaderMap: make(http.Header),
		Body:      new(bytes.Buffer),
	}
}
