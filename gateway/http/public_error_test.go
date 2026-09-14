package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xexec "github.com/viant/xdatly/exec"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

func TestHTTPExplicitErrorResponse(t *testing.T) {
	type input struct{}
	type output struct {
		Message  string
		Internal string
	}
	validation := &xhandler.Validation{Code: 401, Violations: []*xhandler.Violation{{Field: "Name", Message: "denied"}}}
	for _, tc := range []struct {
		name    string
		code    int
		body    any
		failure error
	}{
		{"empty and null", 401, map[string]any{"message": "", "error": nil, "violations": []any{}}, nil},
		{"explicit server body", 500, map[string]any{"message": "public failure", "error": false}, nil},
		{"explicit null", 401, nil, nil},
		{"typed validation", 401, validation, validation.Err()},
		{"negative status", -1, map[string]any{"message": "public failure"}, nil},
		{"oversized status", 1000, map[string]any{"message": "public failure"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := tc.failure
			if failure == nil {
				failure = &response.Error{Code: tc.code, Payload: tc.body, Cause: errors.New("PRIVATE DB CAUSE")}
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/errors", Name: "Check"}, Routes: []*spec.Route{{Method: "GET", Path: "/check"}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{})})
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}), Handler: custom.NewFunc[input, output](func(ctx context.Context, _ *input) (*output, error) {
				if state := xexec.GetContext(ctx); state != nil {
					state.StatusCode = 202
				}
				return &output{Internal: "PRIVATE OUTPUT"}, fmt.Errorf("PRIVATE WRAPPER: %w", failure)
			})}})
			if err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			NewHandler(runtime, nil, "test").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/check", nil))
			want, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatal(err)
			}
			wantCode := tc.code
			if wantCode < 100 || wantCode > 999 {
				wantCode = http.StatusInternalServerError
			}
			if recorder.Code != wantCode || strings.TrimSpace(recorder.Body.String()) != string(want) {
				t.Fatalf("status=%d body=%s want=%s", recorder.Code, recorder.Body.String(), want)
			}
			if strings.Contains(recorder.Body.String(), "PRIVATE") {
				t.Fatal("private cause or output leaked")
			}
		})
	}
}
