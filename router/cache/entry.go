package cache

import (
	"io"
	"net/http"
	"time"
)

type (
	Entry struct {
		meta   Meta
		id     string
		key    uint64
		reader *LineReadCloser

		closed bool
		cache  *Cache
	}

	Meta struct {
		View            string
		Selectors       []byte
		ExpireAt        time.Time
		Size            int
		CompressionType string
		ExtraHeaders    http.Header

		url string
	}
)

func (e *Entry) AddOnClose(func()) {

}

func (e *Entry) Headers() http.Header {
	return e.meta.ExtraHeaders
}

func (e *Entry) Size() int {
	return e.meta.Size
}

func (e *Entry) CompressionType() string {
	return e.meta.CompressionType
}

func (e *Entry) Close() error {
	if e.closed {
		return nil
	}

	e.closed = true
	return e.reader.readCloser.Close()
}

func (e *Entry) Read(p []byte) (int, error) {
	return e.reader.reader.Read(p)
}

func (e *Entry) Reader() (io.Reader, bool) {
	if e.reader != nil {
		return e.reader.reader, true
	}

	return nil, false
}

func (e *Entry) Has() bool {
	return e.reader != nil
}
