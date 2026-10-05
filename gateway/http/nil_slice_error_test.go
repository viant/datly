package http

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xresponse "github.com/viant/xdatly/response"
)

type nilPolicyErrorOutput struct {
	Items  []string `json:"items"`
	Secret string   `json:"secret"`
}
type nilPolicyOtherError nilPolicyErrorOutput

func TestNilSlicePolicyTypedErrorJSON(t *testing.T) {
	for _, policy := range []string{"", "null", "empty_array"} {
		for _, compressed := range []bool{false, true} {
			for _, kind := range []string{"typed", "nil", "map", "other", "raw"} {
				t.Run(policy+"/"+kind+map[bool]string{false: "/plain", true: "/compressed"}[compressed], func(t *testing.T) {
					component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ErrorPolicy", Scope: "example.com/errorpolicy"}, Routes: []*spec.Route{{Method: "GET", Path: "/error"}}, Settings: &spec.Settings{Output: &spec.OutputSettings{NilSlicePolicy: policy, Exclude: []string{"Secret"}}}}
					if compressed {
						component.Settings.ResponseCompression = &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 1}
					}
					artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[nilPolicyErrorOutput]()})
					require.NoError(t, err)
					var payload any
					switch kind {
					case "typed":
						payload = &nilPolicyErrorOutput{Secret: "visible on original error path"}
					case "nil":
						payload = (*nilPolicyErrorOutput)(nil)
					case "map":
						payload = map[string]any{"items": []string(nil), "secret": "unrelated"}
					case "other":
						payload = &nilPolicyOtherError{Secret: "unrelated"}
					case "raw":
						payload = &testStreamResponse{stream: strings.NewReader("opaque RAW"), size: 10}
					}
					rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[nilPolicyErrorOutput](), Handler: custom.NewFunc[struct{}, nilPolicyErrorOutput](func(context.Context, *struct{}) (*nilPolicyErrorOutput, error) {
						return nil, &xresponse.Error{Code: 409, Payload: payload, Cause: errors.New("PRIVATE CAUSE")}
					})}})
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
					rec := httptest.NewRecorder()
					NewHandler(rt, nil, "test").ServeHTTP(rec, httptest.NewRequest("GET", "/error?_format=csv", nil))
					require.Equal(t, 409, rec.Code)
					raw := rec.Body.Bytes()
					if kind == "raw" {
						require.Equal(t, "opaque RAW", string(raw))
						require.Empty(t, rec.Header().Get("Content-Encoding"))
						return
					}
					require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
					if compressed {
						require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
						raw = decodeGzip(t, raw)
					}
					switch kind {
					case "nil":
						require.JSONEq(t, "null", string(raw))
					case "typed":
						if policy == "empty_array" {
							require.JSONEq(t, `{"items":[]}`, string(raw))
						} else {
							require.JSONEq(t, `{"items":null,"secret":"visible on original error path"}`, string(raw))
						}
					default:
						require.JSONEq(t, `{"items":null,"secret":"unrelated"}`, string(raw))
					}
					require.NotContains(t, string(raw), "PRIVATE CAUSE")
				})
			}
		}
	}
}
