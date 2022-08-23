package router

import (
	"bytes"
	"io"
	"net/http"
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

func NewBytesReader(data []byte, compression string) *RequestDataReader {
	return &RequestDataReader{
		buffer:      bytes.NewBuffer(data),
		compression: compression,
		size:        len(data),
		headers:     map[string][]string{},
	}
}

func AsBytesReader(buffer *bytes.Buffer, compression string, size int) *RequestDataReader {
	return &RequestDataReader{
		buffer:      buffer,
		compression: compression,
		size:        size,
		headers:     map[string][]string{},
	}
}
