package router

import (
	"bytes"
	"io"
)

type (
	PayloadReader interface {
		io.Reader
		Size() int
		CompressionType() string
		Close() error
	}

	BytesReader struct {
		buffer      *bytes.Buffer
		compression string
		size        int
	}
)

func (b *BytesReader) Read(p []byte) (n int, err error) {
	return b.buffer.Read(p)
}

func (b *BytesReader) Close() error {
	return nil
}

func (b *BytesReader) Size() int {
	return b.buffer.Len()
}

func (b *BytesReader) CompressionType() string {
	return b.compression
}

func NewBytesReader(data []byte, compression string) *BytesReader {
	return &BytesReader{
		buffer:      bytes.NewBuffer(data),
		compression: compression,
		size:        len(data),
	}
}

func AsBytesReader(buffer *bytes.Buffer, compression string, size int) *BytesReader {
	return &BytesReader{
		buffer:      buffer,
		compression: compression,
		size:        size,
	}
}
