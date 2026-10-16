package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type legacyHeaderInput struct{ Credential string }
type legacyHeaderRow struct {
	ID int `json:"id"`
}
type legacyHeaderOutput struct {
	legacy.Status
	Rows  []*legacyHeaderRow `json:"rows"`
	Other []int              `json:"other"`
}
type legacyHeaderModernOutput struct {
	modern.Status
	Rows []int `json:"rows"`
}

func legacyHeaderHandler(t *testing.T, mode string, policy *spec.ResponseCompression, readerMetadata bool) (*Handler, *atomic.Int64) {
	t.Helper()
	required := true
	calls := new(atomic.Int64)
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}, {Method: "HEAD", Path: "/records"}}, Settings: &spec.Settings{CaseFormat: "lc", ResponseCompression: policy}, Parameters: []*spec.Parameter{{Name: "Credential", Required: &required, ErrorStatusCode: 401, ErrorMessage: "source public missing credential", Source: spec.BindSource{Kind: "header", Name: "Authorization"}}, {Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}, {Name: "Rows", TypeExpr: "[]*legacyHeaderRow", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
	if readerMetadata {
		c.RootView = &spec.View{Name: "records", TypeName: "legacyHeaderRow", Source: &spec.ViewSource{SQL: "SELECT 1 AS ID"}}
	}
	fn := custom.NewFunc[legacyHeaderInput, legacyHeaderOutput](func(context.Context, *legacyHeaderInput) (*legacyHeaderOutput, error) {
		calls.Add(1)
		out := &legacyHeaderOutput{Status: legacy.Status{Status: "ok"}, Rows: []*legacyHeaderRow{{ID: 7}}}
		cause := errors.New("PRIVATE diagnostic")
		switch mode {
		case "success":
			return out, nil
		case "plain":
			return nil, cause
		case "typed":
			out.Status.Status = "error"
			return out, &modern.Error{Code: 409, Cause: cause, Payload: out}
		case "wrapped-typed":
			out.Status.Status = "error"
			return out, fmt.Errorf("private wrapper: %w", &modern.Error{Code: 409, Cause: cause, Payload: out})
		case "joined-typed":
			out.Status.Status = "error"
			return out, errors.Join(&modern.Error{Code: 409, Cause: cause, Payload: out}, errors.New("private joined"))
		case "map":
			return nil, &modern.Error{Code: 409, Cause: cause, Payload: map[string]any{"authored": true}}
		case "nil":
			return nil, &modern.Error{Code: 409, Cause: cause, Payload: nil}
		case "response":
			return nil, &modern.Error{Code: 409, Cause: cause, Payload: modern.NewBuffered(modern.WithBytes([]byte("authored bytes")), modern.WithStatusCode(409), modern.WithHeader("Content-Type", "application/x-authored"), modern.WithHeader("X-Owner", "app"))}
		default:
			t.Fatalf("unknown mode %s", mode)
			return nil, nil
		}
	})
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[legacyHeaderInput](), OutputType: reflect.TypeFor[legacyHeaderOutput](), Handler: fn})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[legacyHeaderOutput](), Handler: fn}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rt.Shutdown(t.Context()) })
	return NewHandler(rt, nil, "legacy-envelope-proof"), calls
}
func TestLegacySynthesizedErrorContentTypeBoundary(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		var policy *spec.ResponseCompression
		if compressed {
			policy = &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 0}
		}
		for _, mode := range []string{"binding", "plain", "success", "typed", "wrapped-typed", "joined-typed", "map", "nil", "response"} {
			t.Run(fmt.Sprintf("gzip%v/%s", compressed, mode), func(t *testing.T) {
				h, calls := legacyHeaderHandler(t, mode, policy, false)
				server := httptest.NewServer(h)
				defer server.Close()
				transport := &stdhttp.Transport{DisableCompression: true}
				defer transport.CloseIdleConnections()
				client := &stdhttp.Client{Transport: transport}
				methods := []string{"GET"}
				if mode == "binding" || mode == "plain" || mode == "typed" || mode == "response" {
					methods = append(methods, "HEAD")
				}
				for _, method := range methods {
					rec := httptest.NewRecorder()
					req := httptest.NewRequest(method, "/records", nil)
					if mode != "binding" {
						req.Header.Set("Authorization", "source test credential")
					}
					h.ServeHTTP(rec, req)
					network, err := stdhttp.NewRequest(method, server.URL+"/records", nil)
					if err != nil {
						t.Fatal(err)
					}
					if mode != "binding" {
						network.Header.Set("Authorization", "source test credential")
					}
					res, err := client.Do(network)
					if err != nil {
						t.Fatal(err)
					}
					wire, err := io.ReadAll(res.Body)
					res.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					status := 409
					contentType := "application/json"
					if mode == "binding" {
						status = 401
						contentType = "text/plain; charset=utf-8"
					}
					if mode == "plain" {
						status = 500
						contentType = "text/plain; charset=utf-8"
					}
					if mode == "success" {
						status = 200
					}
					if mode == "response" {
						contentType = "application/x-authored"
					}
					if rec.Code != status || res.StatusCode != status || rec.Header().Get("Content-Type") != contentType || res.Header.Get("Content-Type") != contentType {
						t.Fatalf("method=%s recorder%d/%s network%d/%s", method, rec.Code, rec.Header().Get("Content-Type"), res.StatusCode, res.Header.Get("Content-Type"))
					}
					if method == "HEAD" {
						if len(wire) != 0 || rec.Body.Len() != 0 || res.ContentLength <= 0 {
							t.Fatalf("HEAD representation changed: %v recbody%d", res.Header, rec.Body.Len())
						}
						continue
					}
					decoded := wire
					if compressed && mode != "response" {
						if res.Header.Get("Content-Encoding") != "gzip" || rec.Header().Get("Content-Encoding") != "gzip" {
							t.Fatal("existing compression missing")
						}
						decoded = decodeGzip(t, wire)
					} else if res.Header.Get("Content-Encoding") != "" {
						t.Fatal("unexpected compression")
					}
					if mode == "nil" && string(decoded) != "null" {
						t.Fatalf("explicit nil %s", decoded)
					}
					if mode == "map" && string(decoded) != `{"authored":true}` {
						t.Fatalf("map owner %s", decoded)
					}
					if mode == "response" && (string(decoded) != "authored bytes" || res.Header.Get("X-Owner") != "app") {
						t.Fatal("explicit Response changed")
					}
					if strings.Contains(string(decoded), "PRIVATE") || strings.Contains(string(decoded), "private wrapper") || strings.Contains(string(decoded), "private joined") {
						t.Fatalf("private diagnostics leaked: %s", decoded)
					}
				}
				if mode == "binding" && calls.Load() != 0 {
					t.Fatal("binding failure executed handler")
				}
			})
		}
	}
}
func TestLegacySynthesizedErrorDeclaredReaderData(t *testing.T) {
	h, calls := legacyHeaderHandler(t, "binding", nil, true)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/records", nil))
	if rec.Code != 401 || rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" || !strings.Contains(rec.Body.String(), `"rows":[]`) || !strings.Contains(rec.Body.String(), `"other":null`) || calls.Load() != 0 {
		t.Fatalf("fresh declared reader envelope %d %s", rec.Code, rec.Body.String())
	}
}
func TestLegacySynthesizedHeaderDoesNotChangeModern(t *testing.T) {
	required := true
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/modern", Name: "modern"}, Routes: []*spec.Route{{Method: "GET", Path: "/modern"}}, Parameters: []*spec.Parameter{{Name: "Credential", Required: &required, ErrorStatusCode: 401, Source: spec.BindSource{Kind: "header", Name: "Authorization"}}, {Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}}}
	fn := custom.NewFunc[legacyHeaderInput, legacyHeaderModernOutput](func(context.Context, *legacyHeaderInput) (*legacyHeaderModernOutput, error) {
		t.Fatal("ran rejected handler")
		return nil, nil
	})
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[legacyHeaderInput](), OutputType: reflect.TypeFor[legacyHeaderModernOutput](), Handler: fn})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[legacyHeaderModernOutput](), Handler: fn}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(t.Context())
	rec := httptest.NewRecorder()
	NewHandler(rt, nil, "modern-proof").ServeHTTP(rec, httptest.NewRequest("GET", "/modern", nil))
	if rec.Code != 401 || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("modern changed %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
}

type legacyHeaderBadOutput struct{ legacy.Status }

func (legacyHeaderBadOutput) MarshalJSON() ([]byte, error) {
	return nil, errors.New("PRIVATE marshal sentinel")
}

func TestLegacySynthesizedEncodingFailureResetsJSON(t *testing.T) {
	required := true
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/encoding", Name: "encoding"}, Routes: []*spec.Route{{Method: "GET", Path: "/encoding"}}, Settings: &spec.Settings{ResponseCompression: &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 0}}, Parameters: []*spec.Parameter{{Name: "Credential", Required: &required, ErrorStatusCode: 401, Source: spec.BindSource{Kind: "header", Name: "Authorization"}}, {Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}}}
	fn := custom.NewFunc[legacyHeaderInput, legacyHeaderBadOutput](func(context.Context, *legacyHeaderInput) (*legacyHeaderBadOutput, error) {
		t.Fatal("binding failure ran handler")
		return nil, nil
	})
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[legacyHeaderInput](), OutputType: reflect.TypeFor[legacyHeaderBadOutput](), Handler: fn})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[legacyHeaderBadOutput](), Handler: fn}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(t.Context())
	h := NewHandler(rt, nil, "encoding-proof")
	preset := stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", "3")
		h.ServeHTTP(w, r)
	})
	rec := httptest.NewRecorder()
	preset.ServeHTTP(rec, httptest.NewRequest("GET", "/encoding", nil))
	if rec.Code != 500 || rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Content-Encoding") != "" || strings.Contains(rec.Body.String(), "PRIVATE") {
		t.Fatalf("fallback recorder %d %v %s", rec.Code, rec.Header(), rec.Body.String())
	}
	server := httptest.NewServer(preset)
	defer server.Close()
	transport := &stdhttp.Transport{DisableCompression: true}
	defer transport.CloseIdleConnections()
	response, err := (&stdhttp.Client{Transport: transport}).Get(server.URL + "/encoding")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 500 || response.Header.Get("Content-Type") != "application/json" || response.Header.Get("Content-Encoding") != "" || string(body) != rec.Body.String() || response.ContentLength != int64(len(body)) {
		t.Fatalf("fallback network %d %v %s", response.StatusCode, response.Header, body)
	}
}
