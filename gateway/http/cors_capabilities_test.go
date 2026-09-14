package http

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

type writerProbe struct {
	headers                             stdhttp.Header
	committed                           stdhttp.Header
	body                                bytes.Buffer
	codes                               []int
	flushed, readFrom, pushed, hijacked int
	err                                 error
}

func (p *writerProbe) Header() stdhttp.Header { return p.headers }
func (p *writerProbe) WriteHeader(code int) {
	p.codes = append(p.codes, code)
	p.committed = p.headers.Clone()
}
func (p *writerProbe) Write(b []byte) (int, error) {
	if len(p.codes) == 0 {
		p.WriteHeader(200)
	}
	return p.body.Write(b)
}
func (p *writerProbe) Flush()                                  { p.flushed++ }
func (p *writerProbe) ReadFrom(r io.Reader) (int64, error)     { p.readFrom++; return p.body.ReadFrom(r) }
func (p *writerProbe) Push(string, *stdhttp.PushOptions) error { p.pushed++; return p.err }
func (p *writerProbe) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	p.hijacked++
	return nil, nil, p.err
}

func (p *writerProbe) surface(mask int) stdhttp.ResponseWriter {
	switch mask {
	case 0:
		return struct{ stdhttp.ResponseWriter }{p}
	case 1:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Flusher
		}{p, p}
	case 2:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Hijacker
		}{p, p}
	case 3:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Flusher
			stdhttp.Hijacker
		}{p, p, p}
	case 4:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Pusher
		}{p, p}
	case 5:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Flusher
			stdhttp.Pusher
		}{p, p, p}
	case 6:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Hijacker
			stdhttp.Pusher
		}{p, p, p}
	case 7:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Flusher
			stdhttp.Hijacker
			stdhttp.Pusher
		}{p, p, p, p}
	case 8:
		return struct {
			stdhttp.ResponseWriter
			io.ReaderFrom
		}{p, p}
	case 9:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Flusher
			io.ReaderFrom
		}{p, p, p}
	case 10:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Hijacker
			io.ReaderFrom
		}{p, p, p}
	case 11:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Flusher
			stdhttp.Hijacker
			io.ReaderFrom
		}{p, p, p, p}
	case 12:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Pusher
			io.ReaderFrom
		}{p, p, p}
	case 13:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Flusher
			stdhttp.Pusher
			io.ReaderFrom
		}{p, p, p, p}
	case 14:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Hijacker
			stdhttp.Pusher
			io.ReaderFrom
		}{p, p, p, p}
	case 15:
		return struct {
			stdhttp.ResponseWriter
			stdhttp.Flusher
			stdhttp.Hijacker
			stdhttp.Pusher
			io.ReaderFrom
		}{p, p, p, p, p}
	}
	panic("invalid surface")
}

func TestCORSWriterPreservesExactCapabilities(t *testing.T) {
	origins := []string{"https://allowed.example"}
	policy, _ := newCORSPolicy(&spec.CORS{AllowOrigins: &origins})
	for mask := 0; mask < 16; mask++ {
		probe := &writerProbe{headers: stdhttp.Header{}, err: errors.New("native result")}
		original := probe.surface(mask)
		req := httptest.NewRequest("GET", "/stream", nil)
		req.Header.Set("Origin", origins[0])
		wrapped := (&corsResponseWriter{ResponseWriter: original, policy: policy, request: req, method: "GET"}).wrap()
		_, flush := wrapped.(stdhttp.Flusher)
		_, hijack := wrapped.(stdhttp.Hijacker)
		_, push := wrapped.(stdhttp.Pusher)
		_, readFrom := wrapped.(io.ReaderFrom)
		for index, actual := range []bool{flush, hijack, push, readFrom} {
			if actual != (mask&(1<<index) != 0) {
				t.Fatalf("mask=%d interface=%d changed", mask, index)
			}
		}
		if wrapped.(interface{ Unwrap() stdhttp.ResponseWriter }).Unwrap() != original {
			t.Fatal("unwrap changed original")
		}
		if hijack {
			if _, _, err := wrapped.(stdhttp.Hijacker).Hijack(); err != probe.err || len(probe.codes) != 0 {
				t.Fatal("hijack semantics changed")
			}
		}
		if push {
			if err := wrapped.(stdhttp.Pusher).Push("/asset", nil); err != probe.err || len(probe.codes) != 0 {
				t.Fatal("push semantics changed")
			}
		}
		wrapped.Header().Set("Access-Control-Allow-Origin", "*")
		if flush {
			wrapped.(stdhttp.Flusher).Flush()
		}
		if readFrom {
			if _, err := wrapped.(io.ReaderFrom).ReadFrom(strings.NewReader("chunk-one")); err != nil {
				t.Fatal(err)
			}
		} else {
			if _, err := wrapped.Write([]byte("chunk-one")); err != nil {
				t.Fatal(err)
			}
		}
		if flush {
			wrapped.(stdhttp.Flusher).Flush()
		}
		if _, err := wrapped.Write([]byte("chunk-two")); err != nil {
			t.Fatal(err)
		}
		if probe.body.String() != "chunk-onechunk-two" || len(probe.codes) != 1 || probe.codes[0] != 200 || probe.committed.Get("Access-Control-Allow-Origin") != origins[0] {
			t.Fatalf("mask=%d stream=%s codes=%v headers=%v", mask, probe.body.String(), probe.codes, probe.committed)
		}
		if readFrom && probe.readFrom != 1 {
			t.Fatal("lost ReaderFrom fast path")
		}
		if flush && probe.flushed != 2 {
			t.Fatal("lost streaming flush")
		}
	}
}

func TestCORSWriterInformationalAndImplicitReadFrom(t *testing.T) {
	origins := []string{"https://allowed.example"}
	policy, _ := newCORSPolicy(&spec.CORS{AllowOrigins: &origins})
	probe := &writerProbe{headers: stdhttp.Header{}}
	req := httptest.NewRequest("GET", "/stream", nil)
	req.Header.Set("Origin", origins[0])
	wrapped := (&corsResponseWriter{ResponseWriter: probe.surface(8), policy: policy, request: req, method: "GET"}).wrap()
	wrapped.WriteHeader(103)
	wrapped.Header().Set("Access-Control-Allow-Origin", "*")
	n, err := io.Copy(wrapped, io.LimitReader(strings.NewReader("stream"), 6))
	if err != nil || n != 6 || probe.readFrom != 1 || len(probe.codes) != 2 || probe.codes[0] != 103 || probe.codes[1] != 200 || probe.committed.Get("Access-Control-Allow-Origin") != origins[0] {
		t.Fatalf("copy=%d/%v calls=%d codes=%v headers=%v", n, err, probe.readFrom, probe.codes, probe.committed)
	}
}
