package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	druntime "github.com/viant/datly/runtime"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type selectorExplicitLegacyReaderOutput struct {
	legacy.Status
	Rows []selectorExplicitRow `json:"data"`
}
type selectorExplicitModernReaderOutput struct {
	modern.Status
	Rows []selectorExplicitRow `json:"data"`
}
type selectorExplicitRow struct {
	ID int `json:"id"`
}
type selectorExplicitInput struct{ Fields []string }

func selectorExplicitComponent(path string) *spec.Component {
	required := false
	return &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "private/system004", Name: "reader"},
		RootView: &spec.View{Name: "Records", Cardinality: "many", Source: &spec.ViewSource{SQL: "SELECT 7 AS id"}},
		Routes:   []*spec.Route{{Method: "GET", Path: path}},
		Parameters: []*spec.Parameter{
			{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}},
			{Name: "Rows", Cardinality: "many", Source: spec.BindSource{Kind: "output", Name: "view"}},
			{Name: "Fields", Required: &required, Source: spec.BindSource{Kind: "query", Name: "fields"}},
		},
	}
}

func selectorExplicitBuild(t *testing.T, path string, output reflect.Type, handler rhandler.TypedHandler) *gateway.Handler {
	t.Helper()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: selectorExplicitComponent(path), InputType: reflect.TypeFor[selectorExplicitInput](), OutputType: output})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: output, Handler: handler}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	return gateway.NewHandler(runtime, nil, "private-control")
}

func TestSelectorExplicitInvalidFieldPayloadOverridesLegacyAndModernOutput(t *testing.T) {
	cause := &dsql.UnknownProjectionColumnError{Column: "missing", RequestedColumn: "missing"}
	payloads := []struct {
		name string
		body any
		want map[string]any
	}{
		{name: "typed", body: modern.Status{Status: "error", Message: "safe selector message"}, want: map[string]any{"status": "error", "message": "safe selector message"}},
		{name: "typed legacy output", body: &selectorExplicitLegacyReaderOutput{Status: legacy.Status{Status: "error", Message: "typed source"}, Rows: []selectorExplicitRow{}}, want: map[string]any{"status": "error", "message": "typed source", "data": []any{}}},
		{name: "typed modern output", body: &selectorExplicitModernReaderOutput{Status: modern.Status{Status: "error", Message: "typed source"}, Rows: []selectorExplicitRow{}}, want: map[string]any{"status": "error", "message": "typed source", "data": []any{}}},
		{name: "arbitrary application map", body: map[string]any{"error": "application-owned", "violations": []any{map[string]any{"field": "filter", "code": "INVALID_FILTER"}}}, want: map[string]any{"error": "application-owned", "violations": []any{map[string]any{"field": "filter", "code": "INVALID_FILTER"}}}},
	}
	outputs := []struct {
		name   string
		typeOf reflect.Type
	}{
		{name: "legacy", typeOf: reflect.TypeFor[selectorExplicitLegacyReaderOutput]()},
		{name: "modern", typeOf: reflect.TypeFor[selectorExplicitModernReaderOutput]()},
	}
	for _, out := range outputs {
		for _, payload := range payloads {
			for _, wrapping := range []string{"direct", "wrapped", "joined"} {
				t.Run(out.name+"/"+payload.name+"/"+wrapping, func(t *testing.T) {
					explicit := &modern.Error{Code: 400, Payload: payload.body, Cause: cause}
					var returned error = explicit
					if wrapping == "wrapped" {
						returned = fmt.Errorf("outer: %w", explicit)
					}
					if wrapping == "joined" {
						returned = errors.Join(explicit, errors.New("private secondary cause"))
					}
					path := "/" + out.name + "?fields=missing"
					var handler *gateway.Handler
					if out.name == "legacy" {
						handler = selectorExplicitBuild(t, "/legacy", out.typeOf, custom.NewFunc[selectorExplicitInput, selectorExplicitLegacyReaderOutput](func(context.Context, *selectorExplicitInput) (*selectorExplicitLegacyReaderOutput, error) {
							return nil, returned
						}))
					} else {
						handler = selectorExplicitBuild(t, "/modern", out.typeOf, custom.NewFunc[selectorExplicitInput, selectorExplicitModernReaderOutput](func(context.Context, *selectorExplicitInput) (*selectorExplicitModernReaderOutput, error) {
							return nil, returned
						}))
					}
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
					if rec.Code != 400 || rec.Header().Get("Content-Type") != "application/json" {
						t.Fatalf("status/media %d %q", rec.Code, rec.Header().Get("Content-Type"))
					}
					var body map[string]any
					if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(body, payload.want) {
						t.Fatalf("explicit payload authority changed: got %#v want %#v", body, payload.want)
					}
					if errors.Is(explicit, cause) == false {
						t.Fatal("cause identity unexpectedly lost")
					}
				})
			}
		}
	}
}
