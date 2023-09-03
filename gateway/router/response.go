package router

import (
	"bytes"
	"github.com/viant/datly/utils/httputils"
	time2 "github.com/viant/datly/utils/time"
	"io"
	"net/http"
	"time"
)

type (
	PayloadReader interface {
		io.Reader
		Size() int
		CompressionType() string
		Close() error
		Headers() http.Header
	}

	RequestDataReader struct {
		buffer      *bytes.Buffer
		compression string
		size        int
		headers     http.Header
	}
)

type RequestDataReaderOption func(r *RequestDataReader)

func WithHeader(name, value string) RequestDataReaderOption {
	return func(r *RequestDataReader) {
		r.headers.Add(name, value)
	}
}

func WithHeaders(header http.Header) RequestDataReaderOption {
	return func(r *RequestDataReader) {
		for key, values := range header {
			header.Set(key, values[0])
		}
	}
}

func (b *RequestDataReader) Read(p []byte) (n int, err error) {
	return b.buffer.Read(p)
}

func (b *RequestDataReader) Close() error {
	return nil
}

func (b *RequestDataReader) Size() int {
	return b.buffer.Len()
}

func (b *RequestDataReader) CompressionType() string {
	return b.compression
}

func (b *RequestDataReader) Headers() http.Header {
	return b.headers
}

func (b *RequestDataReader) AddHeader(name string, value string) {
	b.headers.Add(name, value)
}

func NewBytesReader(data []byte, compression string, options ...RequestDataReaderOption) *RequestDataReader {
	r := &RequestDataReader{
		buffer:      bytes.NewBuffer(data),
		compression: compression,
		size:        len(data),
		headers:     map[string][]string{},
	}

	for _, option := range options {
		option(r)
	}

	return r
}

func AsBytesReader(buffer *bytes.Buffer, compression string, size int) *RequestDataReader {
	return &RequestDataReader{
		buffer:      buffer,
		compression: compression,
		size:        size,
		headers:     map[string][]string{},
	}
}

type ResponseWithMetrics struct {
	startTime time.Time
	http.ResponseWriter
}

func (r *ResponseWithMetrics) Write(data []byte) (int, error) {
	r.writeMetricHeader()

	return r.ResponseWriter.Write(data)
}

func (r *ResponseWithMetrics) WriteHeader(statusCode int) {
	r.writeMetricHeader()

	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *ResponseWithMetrics) writeMetricHeader() {
	r.Header().Set(httputils.DatlyServiceTimeHeader, time2.Since(r.startTime).String())
}

func NewMetricResponseWithTime(writer http.ResponseWriter, start time.Time) *ResponseWithMetrics {
	return &ResponseWithMetrics{
		startTime:      start,
		ResponseWriter: writer,
	}
}

func NewMetricResponse(writer http.ResponseWriter) *ResponseWithMetrics {
	return NewMetricResponseWithTime(writer, time2.Now())
}
