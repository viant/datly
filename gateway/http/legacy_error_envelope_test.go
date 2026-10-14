package http

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type legacyErrorAuthInput struct{ Authorization string }
type legacyErrorModernOutput struct {
	modern.Status
	Data []int `json:"data"`
}
type legacyErrorScalarOutput struct {
	Status string `json:"status"`
	Data   []int  `json:"data"`
}
type legacyErrorNoDeclarationOutput struct {
	legacy.Status
	Data []int `json:"data"`
}

func TestLegacyErrorEnvelopeHTTPModernScalarAndUndeclaredFallbackUnchanged(t *testing.T) {
	required := true
	for _, tc := range []struct {
		name    string
		typ     reflect.Type
		declare bool
	}{
		{"modern", reflect.TypeFor[legacyErrorModernOutput](), true},
		{"scalar", reflect.TypeFor[legacyErrorScalarOutput](), true},
		{"undeclared-legacy", reflect.TypeFor[legacyErrorNoDeclarationOutput](), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, Parameters: []*spec.Parameter{{Name: "Authorization", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required, ErrorStatusCode: 401, ErrorMessage: "authored public missing credential"}}}
			if tc.declare {
				c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}})
			}
			a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[legacyErrorAuthInput](), OutputType: tc.typ})
			if e != nil {
				t.Fatal(e)
			}
			var typed rhandler.TypedHandler
			switch tc.name {
			case "modern":
				typed = custom.NewFunc[legacyErrorAuthInput, legacyErrorModernOutput](func(context.Context, *legacyErrorAuthInput) (*legacyErrorModernOutput, error) {
					t.Error("rejected binding ran handler")
					return nil, nil
				})
			case "scalar":
				typed = custom.NewFunc[legacyErrorAuthInput, legacyErrorScalarOutput](func(context.Context, *legacyErrorAuthInput) (*legacyErrorScalarOutput, error) {
					t.Error("rejected binding ran handler")
					return nil, nil
				})
			default:
				typed = custom.NewFunc[legacyErrorAuthInput, legacyErrorNoDeclarationOutput](func(context.Context, *legacyErrorAuthInput) (*legacyErrorNoDeclarationOutput, error) {
					t.Error("rejected binding ran handler")
					return nil, nil
				})
			}
			rt, e := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: tc.typ, Handler: typed}})
			if e != nil {
				t.Fatal(e)
			}
			defer rt.Shutdown(t.Context())
			rec := httptest.NewRecorder()
			NewHandler(rt, nil, "records-proof").ServeHTTP(rec, httptest.NewRequest("GET", "/records", nil))
			want := `{"status":"error","message":"authored public missing credential","error":"authored public missing credential"}`
			if rec.Code != 401 || strings.TrimSpace(rec.Body.String()) != want {
				t.Fatalf("fallback changed %d %s", rec.Code, rec.Body.String())
			}
			var wire map[string]any
			if e = json.Unmarshal(rec.Body.Bytes(), &wire); e != nil {
				t.Fatal(e)
			}
			if _, ok := wire["data"]; ok {
				t.Fatal("noneligible output root synthesized")
			}
		})
	}
}
