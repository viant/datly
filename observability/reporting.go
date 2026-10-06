package observability

import (
	"bytes"
	"net/http"

	"github.com/viant/gmetric"
)

type metricBuffer struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (b *metricBuffer) Header() http.Header { return b.header }
func (b *metricBuffer) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}
func (b *metricBuffer) Write(value []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.body.Write(value)
}

// ServeMetrics renders a fresh gmetric handler under the same boundary as all
// capture, then releases it before any client I/O. No mutable catalog escapes.
func (r *Recorder) ServeMetrics(prefix string, writer http.ResponseWriter, request *http.Request) {
	buffer := &metricBuffer{header: make(http.Header)}
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		gmetric.NewHandler(prefix, r.service).ServeHTTP(buffer, request)
	}()
	for name, values := range buffer.header {
		writer.Header()[name] = append([]string(nil), values...)
	}
	status := buffer.status
	if status == 0 {
		status = http.StatusOK
	}
	writer.WriteHeader(status)
	_, _ = writer.Write(buffer.body.Bytes())
}
