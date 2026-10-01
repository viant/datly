package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xresponse "github.com/viant/xdatly/response"
)

type compressionInput struct {
	Size int  `parameter:"Size,kind=query,in=size"`
	Fail bool `parameter:"Fail,kind=query,in=fail"`
}
type compressionOutput struct {
	Body string `json:"body"`
}

func compressionServer(t *testing.T, policy *spec.ResponseCompression) *httptest.Server {
	t.Helper()
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/compression", Name: "Bytes"}, Routes: []*spec.Route{{Method: "GET", Path: "/bytes"}, {Method: "HEAD", Path: "/bytes"}}}
	if policy != nil {
		c.Settings = &spec.Settings{ResponseCompression: policy}
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[compressionInput](), OutputType: reflect.TypeFor[compressionOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[compressionOutput](), Handler: custom.NewFunc[compressionInput, compressionOutput](func(_ context.Context, in *compressionInput) (*compressionOutput, error) {
		v := &compressionOutput{Body: strings.Repeat("x", in.Size-11)}
		if in.Fail {
			return nil, &xresponse.Error{Code: 409, Payload: v, Cause: errors.New("PRIVATE CAUSE")}
		}
		return v, nil
	})}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(rt, nil, "test"))
	t.Cleanup(server.Close)
	return server
}
func readWire(t *testing.T, client *stdhttp.Client, method, url, accept string) (*stdhttp.Response, []byte) {
	t.Helper()
	req, err := stdhttp.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if accept != "" {
		req.Header.Set("Accept-Encoding", accept)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if res.Uncompressed {
		t.Fatal("test client decompressed automatically")
	}
	return res, body
}
func decodeGzip(t *testing.T, b []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	v, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestResponseCompressionRealHTTPBoundary(t *testing.T) {
	s := compressionServer(t, &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 2048})
	transport := &stdhttp.Transport{DisableCompression: true}
	defer transport.CloseIdleConnections()
	client := &stdhttp.Client{Transport: transport}
	for _, size := range []int{2047, 2048, 2049} {
		for _, accept := range []string{"", "identity", "gzip"} {
			t.Run(strconv.Itoa(size)+"/"+accept, func(t *testing.T) {
				res, wire := readWire(t, client, "GET", s.URL+"/bytes?size="+strconv.Itoa(size), accept)
				want, _ := json.Marshal(compressionOutput{Body: strings.Repeat("x", size-11)})
				decoded := wire
				if size > 2048 {
					if res.Header.Get("Content-Encoding") != "gzip" || !bytes.Equal(wire[:2], []byte{0x1f, 0x8b}) {
						t.Fatal("large response not gzip")
					}
					decoded = decodeGzip(t, wire)
				} else if res.Header.Get("Content-Encoding") != "" {
					t.Fatal("equal/below boundary was compressed")
				}
				if !bytes.Equal(decoded, want) || len(decoded) != size || res.Header.Get("Content-Length") != strconv.Itoa(len(wire)) || res.StatusCode != 200 {
					t.Fatalf("response status=%d headers=%v wire=%d decoded=%d", res.StatusCode, res.Header, len(wire), len(decoded))
				}
			})
		}
	}
	res, wire := readWire(t, client, "HEAD", s.URL+"/bytes?size=2049", "identity")
	if len(wire) != 0 || res.Header.Get("Content-Encoding") != "gzip" || res.ContentLength <= 0 {
		t.Fatalf("HEAD did not preserve representation metadata: %v %d", res.Header, len(wire))
	}
	res, wire = readWire(t, client, "GET", s.URL+"/bytes?size=2049&fail=true", "")
	want, _ := json.Marshal(compressionOutput{Body: strings.Repeat("x", 2038)})
	if res.StatusCode != 409 || res.Header.Get("Content-Encoding") != "gzip" || !bytes.Equal(decodeGzip(t, wire), want) || bytes.Contains(wire, []byte("PRIVATE")) {
		t.Fatal("error status/payload compression changed")
	}
	unconfigured := compressionServer(t, nil)
	res, wire = readWire(t, client, "GET", unconfigured.URL+"/bytes?size=2049", "")
	if res.Header.Get("Content-Encoding") != "" || len(wire) != 2049 {
		t.Fatal("unconfigured component changed")
	}
}

type observedStream struct {
	reads  int
	source io.Reader
}

func (s *observedStream) Read(b []byte) (int, error) { s.reads++; return s.source.Read(b) }
func TestExplicitResponseStreamAndHeadPreserved(t *testing.T) {
	payload := []byte("application-owned payload")
	var zipped bytes.Buffer
	w := gzip.NewWriter(&zipped)
	w.Write(payload)
	w.Close()
	stream := &observedStream{source: bytes.NewReader(zipped.Bytes())}
	response := xresponse.NewBuffered(xresponse.WithBytes(zipped.Bytes()), xresponse.WithCompressions("gzip"), xresponse.WithHeader("Set-Cookie", "a=1"), xresponse.WithHeader("Set-Cookie", "b=2"), xresponse.WithStatusCode(202))
	rec := httptest.NewRecorder()
	writeResponse(rec, 200, false, response, httptest.NewRequest("HEAD", "/stream", nil))
	if rec.Code != 202 || rec.Body.Len() != 0 || rec.Header().Get("Content-Encoding") != "gzip" || len(rec.Header().Values("Set-Cookie")) != 2 {
		t.Fatal("explicit compressed HEAD changed")
	}
	plain := &testStreamResponse{stream: stream, size: len(zipped.Bytes())}
	rec = httptest.NewRecorder()
	writeResponse(rec, 200, false, plain, httptest.NewRequest("HEAD", "/stream", nil))
	if stream.reads != 0 {
		t.Fatal("HEAD consumed application stream")
	}
	rec = httptest.NewRecorder()
	writeResponse(rec, 200, false, plain, httptest.NewRequest("GET", "/stream", nil))
	if !bytes.Equal(rec.Body.Bytes(), zipped.Bytes()) || rec.Header().Get("Content-Length") != strconv.Itoa(len(zipped.Bytes())) {
		t.Fatal("application stream was buffered or altered")
	}
}

type testStreamResponse struct {
	stream io.Reader
	size   int
}

func (r *testStreamResponse) Body() io.Reader { return r.stream }
func (r *testStreamResponse) Headers() stdhttp.Header {
	return stdhttp.Header{"X-Owner": []string{"application"}}
}
func (r *testStreamResponse) Size() int         { return r.size }
func (r *testStreamResponse) StatusCode() int   { return 200 }
func (r *testStreamResponse) SetStatusCode(int) {}
