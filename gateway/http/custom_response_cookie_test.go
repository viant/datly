package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xresponse "github.com/viant/xdatly/response"
)

type cookieResponseOutput struct{ *xresponse.Buffered }

func TestCustomHandlerCanReturnRedirectAndMultipleCookies(t *testing.T) {
	type input struct {
		State      string `parameter:"State,kind=query,in=state"`
		FlowCookie string `parameter:"FlowCookie,kind=cookie,in=studio_login_state"`
		Origin     string `parameter:"Origin,kind=header,in=Origin"`
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/auth", Name: "Start"},
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/auth/start"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component,
		InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[cookieResponseOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input,
		OutputType: reflect.TypeFor[cookieResponseOutput](),
		Handler: custom.NewFunc[input, cookieResponseOutput](func(_ context.Context, actual *input) (*cookieResponseOutput, error) {
			if actual.State != "approved" || actual.FlowCookie != "encrypted-state" || actual.Origin != "https://studio.example" {
				t.Fatalf("unexpected auth binding: %+v", actual)
			}
			return &cookieResponseOutput{Buffered: xresponse.NewBuffered(
				xresponse.WithStatusCode(http.StatusSeeOther),
				xresponse.WithHeader("Location", "/"),
				xresponse.WithHeader("Set-Cookie", "a=1; HttpOnly"),
				xresponse.WithHeader("Set-Cookie", "b=2; HttpOnly"),
			)}, nil
		})}})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/auth/start?state=approved", nil)
	request.Header.Set("Origin", "https://studio.example")
	request.AddCookie(&http.Cookie{Name: "studio_login_state", Value: "encrypted-state"})
	NewHandler(runtime, nil, "test").ServeHTTP(recorder, request)
	if recorder.Code != http.StatusSeeOther || recorder.Header().Get("Location") != "/" || len(recorder.Header().Values("Set-Cookie")) != 2 {
		t.Fatalf("status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
	}
}
