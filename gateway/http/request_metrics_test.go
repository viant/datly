package http

import (
	"context"
	"fmt"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	internallog "github.com/viant/datly/internal/logging"
	"github.com/viant/datly/observability"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xexec "github.com/viant/xdatly/exec"
	xresponse "github.com/viant/xdatly/response"
)

func TestRequestMetricsObserveActualHTTPOutcome(t *testing.T) {
	type input struct{}
	type useCase struct {
		description string
		status      int
		transport   bool
	}
	for _, tc := range []useCase{{"success", 200, false}, {"validation body with nil handler error", 400, false}, {"server response", 500, false}, {"redirect", 303, false}, {"transport error after committed success", 200, true}} {
		for _, outer := range []bool{false, true} {
			for _, audit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/outer=%v/audit=%v", tc.description, outer, audit), func(t *testing.T) {
					component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "Write"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/records/{id}"}}}
					artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[cookieResponseOutput]()})
					require.NoError(t, err)
					policy := &observability.Policy{Requests: []observability.RequestObservation{{Route: spec.RouteRef{Method: "PATCH", Path: "/records/{id}"}, Operation: observability.OperationDescriptor{Name: "PATCH:example.records.T.request", Location: "example/records", Description: "records request"}}}}
					rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[cookieResponseOutput](), Handler: custom.NewFunc[input, cookieResponseOutput](func(context.Context, *input) (*cookieResponseOutput, error) {
						return &cookieResponseOutput{Buffered: xresponse.NewBuffered(xresponse.WithStatusCode(tc.status), xresponse.WithBytes([]byte(`{"status":"unchanged"}`)))}, nil
					})}}, druntime.WithObservability(druntime.ObservabilityConfig{Policy: policy, Logging: &observability.Logging{EnableAudit: &audit}}))
					require.NoError(t, err)
					defer rt.Shutdown(context.Background())
					h := NewHandler(rt, nil, "test")
					result := httptest.NewRecorder()
					var writer stdhttp.ResponseWriter = result
					if tc.transport {
						writer = &failingHTTPWriter{header: stdhttp.Header{}}
					}
					request := httptest.NewRequest("PATCH", "/records/17", nil)
					if outer {
						internallog.ServeHTTPObserved(writer, request, "test", h.ServeHTTP, func(context.Context, *xexec.Context) { panic("logging failure must not lose metrics") })
					} else {
						h.ServeHTTP(writer, request)
					}
					if !tc.transport {
						require.Equal(t, tc.status, result.Code)
						require.JSONEq(t, `{"status":"unchanged"}`, result.Body.String())
					}
					r := rt.Observability().Recorder
					name := "PATCH:example.records.T.request"
					require.EqualValues(t, 1, r.Cumulative(name, "Request"))
					require.EqualValues(t, 1, r.Cumulative(name, "count"))
					failed := tc.status >= 400 || tc.transport
					require.EqualValues(t, map[bool]int{true: 1}[failed], r.Cumulative(name, "Error"))
					require.EqualValues(t, map[bool]int{false: 1}[failed], r.Cumulative(name, "Success"))
					for label, expected := range map[string]bool{"status:2xx": tc.status >= 200 && tc.status < 300, "status:4xx": tc.status >= 400 && tc.status < 500, "status:5xx": tc.status >= 500} {
						require.EqualValues(t, map[bool]int{true: 1}[expected], r.Cumulative(name, label), label)
					}
				})
			}
		}
	}
}
