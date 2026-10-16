package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	dauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xcodec "github.com/viant/xdatly/codec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

func TestOpaqueVerifierVisibleExplicitBodyGatewayPrecedence(t *testing.T) {
	service, err := dauth.New(t.Context(), &dauth.Config{JWTValidator: &verifier.Config{CertURL: "private-diagnostic://invalid/EXPLICIT_PRIVATE_SENTINEL"}})
	if err != nil {
		t.Fatal(err)
	}
	codec, err := service.New(&xcodec.Config{Body: "JwtClaim", SourceType: reflect.TypeFor[string](), DestinationType: reflect.TypeFor[*jwt.Claims]()})
	if err != nil {
		t.Fatal(err)
	}
	_, failure := codec.Value(t.Context(), "non-secret-input")
	var verified *dauth.VerificationFailure
	if !errors.As(failure, &verified) {
		t.Fatal("genuine configured verifier failure absent")
	}
	type input struct{}
	type output struct {
		legacy.Status
		Data []struct {
			ID int `sqlx:"id"`
		}
	}
	for _, body := range []any{nil, map[string]any{"safe": false, "values": []int{}}, &struct {
		Public string `json:"public"`
	}{"typed"}} {
		for _, shape := range []string{"outer", "wrapped", "joined", "reversed joined"} {
			t.Run(fmt.Sprintf("%T/%s", body, shape), func(t *testing.T) {
				owner := &bindly.BindingError{Name: "Credential", Kind: "header", Code: 401, Cause: verified}
				explicit := &modern.Error{Code: 401, Payload: body, Cause: owner}
				var cause error = explicit
				switch shape {
				case "wrapped":
					cause = fmt.Errorf("private wrapper: %w", explicit)
				case "joined":
					cause = errors.Join(owner, explicit)
				case "reversed joined":
					cause = errors.Join(explicit, owner)
				}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/public", Name: "records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "RecordsCase", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}, Parameters: []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}}}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output]()})
				if err != nil {
					t.Fatal(err)
				}
				native, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[output](), Handler: custom.NewFunc[input, output](func(context.Context, *input) (*output, error) { return nil, cause })}})
				if err != nil {
					t.Fatal(err)
				}
				defer native.Shutdown(t.Context())
				result := httptest.NewRecorder()
				NewHandler(native, nil, "explicit-opaque").ServeHTTP(result, httptest.NewRequest("GET", "/records", nil))
				want, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				if result.Code != 401 || strings.TrimSpace(result.Body.String()) != string(want) {
					t.Fatal("genuine explicit nil/map/typed body changed by projector")
				}
			})
		}
	}
}
