package http

import (
	"context"
	"encoding/json"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xasync "github.com/viant/xdatly/async"
)

type routingConsumerInput struct {
	ID string `parameter:"ID,kind=path,in=id"`
}
type routingConsumerOutput struct {
	ID    string `json:"id"`
	Route string `json:"route"`
}

func routingConsumerRuntime(t *testing.T) *druntime.Runtime {
	t.Helper()
	var components []*registry.RegisteredComponent
	for i, path := range []string{"/things/{id}", "/things/a/{id}"} {
		label := []string{"escaped", "decoded"}[i]
		origins := []string{"https://" + label + ".example"}
		route := &spec.Route{Method: "GET", Path: path, APIKeyHeader: "X-Key", APIKeyValue: label, CORS: &spec.CORS{AllowOrigins: &origins}}
		component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/routing", Name: label}, Routes: []*spec.Route{route}}
		if i == 1 {
			component.Settings = &spec.Settings{ResponseCompression: &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 1}}
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[routingConsumerInput](), OutputType: reflect.TypeFor[routingConsumerOutput]()})
		if err != nil {
			t.Fatal(err)
		}
		components = append(components, &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[routingConsumerOutput](), Handler: custom.NewFunc[routingConsumerInput, routingConsumerOutput](func(_ context.Context, in *routingConsumerInput) (*routingConsumerOutput, error) {
			return &routingConsumerOutput{ID: in.ID, Route: label}, nil
		})})
	}
	rt, err := druntime.NewRuntime(components)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func TestRoutingConsumersSelectAndBindSamePath(t *testing.T) {
	rt := routingConsumerRuntime(t)
	for _, mode := range []string{"", "escaped", "decoded"} {
		t.Run(mode, func(t *testing.T) {
			h, err := (Config{PathSemantics: mode}).NewHandler(rt, nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			expected, id := "escaped", "a/b"
			if mode == "decoded" {
				expected, id = "decoded", "b"
			}
			req := httptest.NewRequest("GET", "/things/a%2Fb?ID=spoof", nil)
			req.Header.Set("X-Key", expected)
			originalURL, originalURI := *req.URL, req.RequestURI
			// Each HTTP exchange owns its body; share only the immutable original URL.
			concurrent := req.Clone(req.Context())
			concurrent.URL = req.URL
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, concurrent)
			if rec.Code != 200 {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			body := rec.Body.Bytes()
			if expected == "decoded" {
				if rec.Header().Get("Content-Encoding") != "gzip" {
					t.Fatal("selected compression policy missing")
				}
				body = decodeGzip(t, body)
			} else if rec.Header().Get("Content-Encoding") != "" {
				t.Fatal("wrong compression owner")
			}
			var out routingConsumerOutput
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatal(err)
			}
			if out.ID != id || out.Route != expected {
				t.Fatalf("output=%+v", out)
			}
			if *req.URL != originalURL || req.RequestURI != originalURI {
				t.Fatal("request URL mutated")
			}
			req.Header.Set("X-Key", "wrong")
			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != 403 {
				t.Fatalf("wrong selected key status=%d", rec.Code)
			}
			req.Method = "POST"
			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != 405 || rec.Header().Get("Allow") != "GET" {
				t.Fatalf("method status=%d Allow=%s", rec.Code, rec.Header().Get("Allow"))
			}
			req.Method = "OPTIONS"
			req.Header.Set("Origin", "https://"+expected+".example")
			req.Header.Set("Access-Control-Request-Method", "GET")
			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != 204 || rec.Header().Get("Access-Control-Allow-Origin") == "" {
				t.Fatalf("preflight=%d headers=%v", rec.Code, rec.Header())
			}
		})
	}
}

func TestRoutingConsumersStaticGuardsPreserveOriginalEncodings(t *testing.T) {
	rt := routingConsumerRuntime(t)
	for _, mode := range []string{"", "escaped", "decoded"} {
		h, err := (Config{PathSemantics: mode}).NewHandler(rt, nil, "test")
		if err != nil {
			t.Fatal(err)
		}
		h.static = []*staticRoute{{prefix: "/assets", content: &spec.StaticContent{}, handler: stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) { w.WriteHeader(200) })}}
		for _, path := range []string{"/assets/a%2fb", "/assets/a%2Fb", "/assets/a%5cb", "/assets/a%5Cb"} {
			t.Run(mode+path, func(t *testing.T) {
				req := httptest.NewRequest("GET", path, nil)
				before, uri := *req.URL, req.RequestURI
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != 404 {
					t.Fatalf("unsafe static=%d", rec.Code)
				}
				if *req.URL != before || req.RequestURI != uri {
					t.Fatal("original static evidence mutated")
				}
			})
		}
		// A decoded component owns its path even when the original escaped path has no native match.
		h.static[0].prefix = "/things"
		req := httptest.NewRequest("POST", "/things%2Fa/b", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		want := 404
		if mode == "decoded" {
			want = 405
		}
		if rec.Code != want {
			t.Fatalf("component precedence=%d want=%d", rec.Code, want)
		}
	}
}

type routingSubmissionProbe struct{ submission jobs.Submission }

func (p *routingSubmissionProbe) Exchange(_ context.Context, s jobs.Submission) (*jobs.Exchange, error) {
	p.submission = s
	return nil, nil
}
func TestRoutingConsumersAsyncCanonicalURIAndNativeOwnership(t *testing.T) {
	rt := routingConsumerRuntime(t)
	for _, mode := range []string{"", "escaped", "decoded"} {
		h, err := (Config{PathSemantics: mode}).NewHandler(rt, nil, "test")
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest("GET", "/things/a%2Fb?name=a%2Fb&name=c", nil)
		before, uri := *req.URL, req.RequestURI
		r := &asyncRoute{}
		targetPath := "/things/{id}"
		if mode == "decoded" {
			targetPath = "/things/a/{id}"
		}
		h.async = &asyncRoutes{routes: map[string]*asyncRoute{(spec.RouteRef{Method: "GET", Path: targetPath}).String(): r}}
		if h.asyncRoute(req) != r {
			t.Fatal("HTTP async route owner differs from selected path")
		}
		p := &routingSubmissionProbe{}
		if _, err := r.execute(context.Background(), req, h.routingPath(req), nil, p); err != nil {
			t.Fatal(err)
		}
		expected := "/things/a%2Fb?name=a%2Fb&name=c"
		target := "/things/{id}"
		if mode == "decoded" {
			expected = "/things/a/b?name=a%2Fb&name=c"
			target = "/things/a/{id}"
		}
		if p.submission.Job.URI != expected {
			t.Fatalf("stored URI=%s", p.submission.Job.URI)
		}
		if !h.ownsAsyncJob(spec.RouteRef{Method: "GET", Path: target}, &p.submission.Job) {
			t.Fatal("canonical job replay has different owner")
		}
		direct := &xasync.Job{Request: xasync.Request{Method: "GET", URI: "/things/a%2Fb"}}
		if !h.ownsAsyncJob(spec.RouteRef{Method: "GET", Path: "/things/{id}"}, direct) {
			t.Fatal("native direct job semantics changed")
		}
		if *req.URL != before || req.RequestURI != uri {
			t.Fatal("async request mutated")
		}
	}
}

func TestRoutingConsumersConcurrentRequestPreservation(t *testing.T) {
	h, err := (Config{PathSemantics: "decoded"}).NewHandler(routingConsumerRuntime(t), nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/things/a%2Fb?key=x%2Fy", nil)
	req.Header.Set("X-Key", "decoded")
	before, uri := *req.URL, req.RequestURI
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each HTTP exchange owns its body; share only the immutable original URL.
			concurrent := req.Clone(req.Context())
			concurrent.URL = req.URL
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, concurrent)
			if rec.Code != 200 {
				t.Errorf("status=%d", rec.Code)
			}
		}()
	}
	wg.Wait()
	if *req.URL != before || req.RequestURI != uri || !strings.Contains(req.URL.RawPath, "%2F") {
		t.Fatal("concurrent original request changed")
	}
}
