package support

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type responseLimitFailureKey struct{}

// WithResponseLimitFailure lets a protocol wait be canceled with the read
// failure when its transport does not propagate streaming body errors itself.
func WithResponseLimitFailure(ctx context.Context, fail func(error)) context.Context {
	return context.WithValue(ctx, responseLimitFailureKey{}, fail)
}

// NewBoundedTransportClient applies a limit to each HTTP response body before
// a protocol decoder can buffer it. Streaming responses share the limit across
// their complete body; the limit is not reset for individual Read calls.
func NewBoundedTransportClient(limit int64) *http.Client {
	client := NewTransportClient()
	if limit == 0 || limit == int64(^uint64(0)>>1) {
		return client
	}
	client.Transport = &responseLimitTransport{base: client.Transport, limit: limit}
	return client
}

type responseLimitTransport struct {
	base  http.RoundTripper
	limit int64
}

func (t *responseLimitTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return response, err
	}
	if response.ContentLength > t.limit {
		_ = response.Body.Close()
		return nil, fmt.Errorf("response exceeds %d bytes", t.limit)
	}
	// The MCP JSON transport currently ignores body-read errors. Validate the
	// complete bounded JSON body before handing it to that decoder, so a
	// truncated oversized document can never be treated as a notification.
	if !strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		data, readErr := io.ReadAll(io.LimitReader(response.Body, t.limit+1))
		_ = response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if int64(len(data)) > t.limit {
			return nil, fmt.Errorf("response exceeds %d bytes", t.limit)
		}
		response.Body = io.NopCloser(bytes.NewReader(data))
		return response, nil
	}
	fail, _ := request.Context().Value(responseLimitFailureKey{}).(func(error))
	response.Body = &limitedResponseBody{ReadCloser: response.Body, remaining: t.limit, limit: t.limit, fail: fail}
	return response, nil
}

func (t *responseLimitTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

type limitedResponseBody struct {
	io.ReadCloser
	remaining int64
	limit     int64
	exceeded  bool
	fail      func(error)
}

func (b *limitedResponseBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.exceeded {
		return 0, fmt.Errorf("response exceeds %d bytes", b.limit)
	}
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(p)
	if int64(n) > b.remaining {
		b.exceeded = true
		err := fmt.Errorf("response exceeds %d bytes", b.limit)
		if b.fail != nil {
			b.fail(err)
		}
		return 0, err
	}
	b.remaining -= int64(n)
	return n, err
}
