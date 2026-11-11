package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	legacyresponse "github.com/viant/xdatly/handler/response"
	xresponse "github.com/viant/xdatly/response"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
)

type staticHeaderInput struct{}
type staticHeaderOutput struct {
	URL string `json:"url"`
}

func TestTypedStaticSuccessHeaders(t *testing.T) {
	settings, err := tag.ParseSettings(reflect.StructTag(`output:"{\"headers\":{\"Cache-Control\":\"no-store, private\",\"Pragma\":\"no-cache\",\"X-Content-Type-Options\":\"nosniff\"}}"`))
	if err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/report", Name: "Report"}, Routes: []*spec.Route{{Method: "GET", Path: "/report"}, {Method: "HEAD", Path: "/report"}}, Settings: &spec.Settings{}}
	settings.Apply(c.Settings)
	fn := custom.NewFunc[staticHeaderInput, staticHeaderOutput](func(context.Context, *staticHeaderInput) (*staticHeaderOutput, error) {
		return &staticHeaderOutput{URL: "https://example.invalid/report"}, nil
	})
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[staticHeaderInput](), OutputType: reflect.TypeFor[staticHeaderOutput](), Handler: fn})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[staticHeaderOutput](), Handler: fn}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	for _, method := range []string{"GET", "HEAD"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(method, "/report", nil)
		req.Header.Set("X-Request-Secret", "must-not-echo")
		NewHandler(rt, nil, "test").ServeHTTP(rec, req)
		if rec.Code != 200 || rec.Header().Get("Cache-Control") != "no-store, private" || rec.Header().Get("Pragma") != "no-cache" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("%s status=%d headers=%v body=%s", method, rec.Code, rec.Header(), rec.Body.String())
		}
		if rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("X-Request-Secret") != "" {
			t.Fatal(rec.Header())
		}
		if method == "HEAD" {
			if rec.Body.Len() != 0 {
				t.Fatal("HEAD body")
			}
			continue
		}
		var body map[string]string
		if err = json.Unmarshal(rec.Body.Bytes(), &body); err != nil || len(body) != 1 || body["url"] != "https://example.invalid/report" {
			t.Fatalf("typed body changed: %v %s", err, rec.Body.String())
		}
	}
}

type guardedHeaderInput struct {
	Token string `parameter:"Token,kind=header,in=Authorization,required=true,errorCode=401"`
	Mode  string `parameter:"Mode,kind=query,in=mode"`
}

func TestTypedStaticHeadersDoNotLeakAcrossErrorsOrRequests(t *testing.T) {
	var calls atomic.Int32
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/report", Name: "Guarded"}, Routes: []*spec.Route{{Method: "GET", Path: "/guarded"}}, Settings: &spec.Settings{Output: &spec.OutputSettings{Headers: map[string]string{"Cache-Control": "no-store"}}, ResponseCompression: &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 0}}}
	fn := custom.NewFunc[guardedHeaderInput, staticHeaderOutput](func(_ context.Context, in *guardedHeaderInput) (*staticHeaderOutput, error) {
		calls.Add(1)
		switch in.Mode {
		case "403":
			return nil, &xresponse.Error{Code: 403, Payload: &staticHeaderOutput{URL: "denied"}}
		case "404":
			return nil, &legacyresponse.Error{Code: 404, Message: "not available"}
		case "500":
			return nil, errors.New("storage failure")
		}
		return &staticHeaderOutput{URL: "https://example.invalid/report"}, nil
	})
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[guardedHeaderInput](), OutputType: reflect.TypeFor[staticHeaderOutput](), Handler: fn})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[staticHeaderOutput](), Handler: fn}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	h := NewHandler(rt, nil, "test")
	check := func(mode string, token bool, want int) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/guarded?mode="+mode, nil)
		if token {
			req.Header.Set("Authorization", "fixture")
		}
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("mode%s status%d body%s", mode, rec.Code, rec.Body.String())
		}
		expected := ""
		if want == 200 {
			expected = "no-store"
			if rec.Header().Get("Content-Encoding") != "gzip" {
				t.Error("compression lost")
			}
		}
		if rec.Header().Get("Cache-Control") != expected {
			t.Errorf("mode%s headers%v", mode, rec.Header())
		}
	}
	check("", false, 401)
	if calls.Load() != 0 {
		t.Fatal("auth invoked business")
	}
	var wg sync.WaitGroup
	for range 5 {
		for _, mode := range []string{"", "403", "404", "500"} {
			wg.Add(1)
			go func(mode string) {
				defer wg.Done()
				want := 200
				if mode != "" {
					want, _ = strconv.Atoi(mode)
				}
				check(mode, true, want)
			}(mode)
		}
	}
	wg.Wait()
}

// A deliberately failing existing JSON contract exercises the pre-commit error
// boundary; application success uses the plain typed struct above.
type failingHeaderOutput struct{ URL string }

func (*failingHeaderOutput) MarshalJSON() ([]byte, error) {
	return nil, errors.New("fixture encoding failure")
}
func TestStaticSuccessHeadersAbsentAfterEncodingFailure(t *testing.T) {
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/report", Name: "Bad"}, Routes: []*spec.Route{{Method: "GET", Path: "/bad"}}, Settings: &spec.Settings{Output: &spec.OutputSettings{Headers: map[string]string{"Cache-Control": "no-store"}}}}
	fn := custom.NewFunc[staticHeaderInput, failingHeaderOutput](func(context.Context, *staticHeaderInput) (*failingHeaderOutput, error) {
		return &failingHeaderOutput{URL: "private"}, nil
	})
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[staticHeaderInput](), OutputType: reflect.TypeFor[failingHeaderOutput](), Handler: fn})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[failingHeaderOutput](), Handler: fn}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	rec := httptest.NewRecorder()
	NewHandler(rt, nil, "test").ServeHTTP(rec, httptest.NewRequest("GET", "/bad", nil))
	if rec.Code != 500 || rec.Header().Get("Cache-Control") != "" || strings.Contains(rec.Body.String(), "private") {
		t.Fatalf("status%d headers%v body%s", rec.Code, rec.Header(), rec.Body.String())
	}
}

func TestStaticHeadersStayOnFinalHTTPComponent(t *testing.T) {
	var rt *druntime.Runtime
	child := custom.NewFunc[staticHeaderInput, staticHeaderOutput](func(context.Context, *staticHeaderInput) (*staticHeaderOutput, error) {
		return &staticHeaderOutput{URL: "child"}, nil
	})
	parent := custom.NewFunc[staticHeaderInput, staticHeaderOutput](func(ctx context.Context, _ *staticHeaderInput) (*staticHeaderOutput, error) {
		got, err := rt.ExecuteRoute(ctx, "GET", "/child", nil)
		if err != nil {
			return nil, err
		}
		if result, ok := got.(*staticHeaderOutput); !ok || result.URL != "child" {
			return nil, fmt.Errorf("unexpected child %T", got)
		}
		return &staticHeaderOutput{URL: "parent"}, nil
	})
	var entries []*registry.RegisteredComponent
	for _, name := range []string{"child", "parent"} {
		fn := child
		settings := &spec.Settings{Output: &spec.OutputSettings{Headers: map[string]string{"X-Child-Only": "private-child-policy"}}}
		if name == "parent" {
			fn = parent
			settings = &spec.Settings{}
		}
		c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/headers", Name: name}, Routes: []*spec.Route{{Method: "GET", Path: "/" + name}}, Settings: settings}
		a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[staticHeaderInput](), OutputType: reflect.TypeFor[staticHeaderOutput](), Handler: fn})
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[staticHeaderOutput](), Handler: fn})
	}
	var err error
	rt, err = druntime.NewRuntime(entries)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	h := NewHandler(rt, nil, "test")
	for _, path := range []string{"/child", "/parent", "/parent", "/child"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		expected := ""
		if path == "/child" {
			expected = "private-child-policy"
		}
		if rec.Code != 200 || rec.Header().Get("X-Child-Only") != expected {
			t.Fatalf("%s status%d headers%v body%s", path, rec.Code, rec.Header(), rec.Body.String())
		}
	}
}
