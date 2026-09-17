package http

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestHTTPDateFormatBindingStatus(t *testing.T) {
	type input struct {
		From *time.Time `format:"dateFormat=YYYY-MM-DD"`
	}
	type output struct{ OK bool }
	for _, source := range []string{"form", "query"} {
		for _, tc := range []struct {
			name, value      string
			status, authored int
			absent           bool
		}{
			{name: "valid", value: "2026-09-15", status: 200},
			{name: "invalid", value: "2026-09-99", status: 400},
			{name: "empty", status: 400},
			{name: "absent", status: 200, absent: true},
			{name: "authored-status", value: "2026-09-99", status: 422, authored: 422},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				component := &spec.Component{
					Key:        spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Dates"},
					Routes:     []*spec.Route{{Method: "GET", Path: "/dates"}},
					Parameters: []*spec.Parameter{{Name: "From", Source: spec.BindSource{Kind: source, Name: "from"}, ErrorStatusCode: tc.authored}},
				}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output]()})
				require.NoError(t, err)
				called := false
				rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{
					Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](),
					Handler: custom.NewFunc[input, output](func(_ context.Context, in *input) (*output, error) {
						called = true
						if tc.absent {
							require.Nil(t, in.From)
						} else {
							require.Equal(t, "2026-09-15", in.From.Format("2006-01-02"))
						}
						return &output{OK: true}, nil
					}),
				}})
				require.NoError(t, err)
				log := &securityLog{}
				h := NewHandler(rt, log, "test")
				uri := "/dates"
				if !tc.absent {
					uri += "?from=" + tc.value
				}
				response := httptest.NewRecorder()
				h.ServeHTTP(response, httptest.NewRequest("GET", uri, nil))
				require.Equal(t, tc.status, response.Code, response.Body.String())
				require.Equal(t, tc.status == 200, called, "invalid input must not reach execution")
				if tc.status != 200 {
					var parse *time.ParseError
					require.ErrorAs(t, log.failure, &parse, "retain private parse cause in diagnostics")
					require.False(t, strings.Contains(response.Body.String(), "2026-09-99"))
					require.NotContains(t, response.Body.String(), "parsing time")
				}
			})
		}
	}
}
