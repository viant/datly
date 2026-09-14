package response

import (
	"bytes"
	"io"
	"net/http"
)

// Buffered is the stable in-memory convenience implementation of Response.
// It stays public because handlers and runtimes may need one transport-ready
// response value without depending on a specific HTTP framework.
type Buffered struct {
	buffer      *bytes.Buffer
	statusCode  int
	compression string
	size        int
	headers     http.Header
}

func (b *Buffered) StatusCode() int {
	return b.statusCode
}

func (b *Buffered) Body() io.Reader {
	if b == nil || b.buffer == nil {
		return nil
	}
	return bytes.NewReader(b.buffer.Bytes())
}

func (b *Buffered) Headers() http.Header {
	if b == nil {
		return nil
	}
	return b.headers
}

func (b *Buffered) Size() int {
	if b == nil {
		return 0
	}
	return b.size
}

func (b *Buffered) CompressionType() string {
	if b == nil {
		return ""
	}
	return b.compression
}

func (b *Buffered) SetStatusCode(status int) {
	if b == nil {
		return
	}
	b.statusCode = status
}

func (b *Buffered) Write(data []byte) (int, error) {
	if b == nil {
		return 0, nil
	}
	if b.buffer == nil {
		b.buffer = &bytes.Buffer{}
	}
	written, err := b.buffer.Write(data)
	b.size = b.buffer.Len()
	return written, err
}

func NewBuffered(options ...Option) *Buffered {
	response := &Buffered{}
	for _, option := range options {
		if option != nil {
			option(response)
		}
	}
	if response.buffer == nil {
		response.buffer = &bytes.Buffer{}
	}
	if response.headers == nil {
		response.headers = make(http.Header)
	}
	if response.size == 0 {
		response.size = response.buffer.Len()
	}
	return response
}

// Option configures a buffered response.
type Option func(r *Buffered)

// Options is a buffered response option list.
type Options []Option

// AdjustStatusCode applies the first available explicit status code candidates.
func (o *Options) AdjustStatusCode(candidates ...any) {
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if coder, ok := candidate.(StatusCoder); ok {
			*o = append(*o, WithStatusCode(coder.StatusCode()))
		}
	}
}

// Append appends options.
func (o *Options) Append(opts ...Option) {
	*o = append(*o, opts...)
}

// Options returns the underlying list.
func (o *Options) Options() Options {
	return *o
}

func WithStatusCode(statusCode int) Option {
	return func(r *Buffered) {
		r.statusCode = statusCode
	}
}

func WithBytes(data []byte) Option {
	return func(r *Buffered) {
		r.buffer = bytes.NewBuffer(data)
		r.size = len(data)
	}
}

func WithBuffer(buffer *bytes.Buffer) Option {
	return func(r *Buffered) {
		r.buffer = buffer
		if buffer != nil {
			r.size = buffer.Len()
		}
	}
}

func WithCompressionType(compression string) Option {
	return func(r *Buffered) {
		r.compression = compression
	}
}

// WithCompressions preserves the older option spelling used by existing call
// sites while keeping the owner under the response transport package.
func WithCompressions(compression string) Option {
	return WithCompressionType(compression)
}

func WithSize(size int) Option {
	return func(r *Buffered) {
		r.size = size
	}
}

func WithHeader(name, value string) Option {
	return func(r *Buffered) {
		if r.headers == nil {
			r.headers = make(http.Header)
		}
		r.headers.Add(name, value)
	}
}

func WithHeaders(header http.Header) Option {
	return func(r *Buffered) {
		if r.headers == nil {
			r.headers = make(http.Header)
		}
		for key, values := range header {
			r.headers.Del(key)
			for _, value := range values {
				r.headers.Add(key, value)
			}
		}
	}
}
