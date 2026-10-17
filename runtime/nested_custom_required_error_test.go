package runtime_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	rhandler "github.com/viant/datly/runtime/handler"
	custom "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xhandler "github.com/viant/xdatly/handler"
)

type nestedCustomRequiredOutput struct {
	Fields []string `json:"fields"`
}

// Unlike the reader counterpart, this outer custom handler has no declared
// legacy error envelope. Binding must still preserve safe absence text and
// redact verifier diagnostics before either handler or Init executes.
type optionalParentCredentialInput struct {
	nestedOwnerInput
	Credential *jwt.Claims `parameter:"Credential,kind=header,in=X-Credential,dataType=string,required=false,errorCode=401" codec:"JwtClaim"`
}

func TestNestedCustomRequiredCredentialHTTP(t *testing.T) {
	testNestedCustomRequiredCredentialHTTP(t, reflect.TypeFor[nestedOwnerInput](), false)
}

func TestNestedCustomRequiredCredentialWithOptionalParentHTTP(t *testing.T) {
	testNestedCustomRequiredCredentialHTTP(t, reflect.TypeFor[optionalParentCredentialInput](), false)
}

type scopedRequiredCredentialInput nestedOwnerCredentialInput

type scopedRequiredCredentialContract struct{ t *testing.T }

func (scopedRequiredCredentialContract) BindsInputByScope() bool { return true }
func (c scopedRequiredCredentialContract) Exec(context.Context, xhandler.Session, *scopedRequiredCredentialInput, *nestedOwnerCredentialOutput) error {
	c.t.Fatal("rejected scoped child executed")
	return nil
}
func TestNestedScopedRequiredCredentialHTTP(t *testing.T) {
	testNestedCustomRequiredCredentialHTTP(t, reflect.TypeFor[optionalParentCredentialInput](), true)
}
func testNestedCustomRequiredCredentialHTTP(t *testing.T, inputType reflect.Type, scoped bool) {
	key, wrong := testharness.NewJWT(t), testharness.NewJWT(t)
	for _, retain := range []bool{false, true} {
		for _, tc := range []struct {
			name, credential string
			fetch            bool
		}{
			{"missing", "", false},
			{"malformed", "Bearer invalid", false},
			{"expired", key.Sign(t, "record-user", time.Now().Add(-time.Hour)), false},
			{"wrong signature", wrong.Sign(t, "record-user", time.Now().Add(time.Hour)), false},
			{"configured certificate fetch", "non-secret-generic-input", true},
		} {
			t.Run(tc.name+map[bool]string{false: "/discard", true: "/retain"}[retain], func(t *testing.T) {
				cfg := key.Config()
				if tc.fetch {
					cfg = &verifier.Config{CertURL: "private-diagnostic://invalid/GENERIC_PRIVATE_SENTINEL"}
				}
				auth, err := runtimeauth.New(t.Context(), &runtimeauth.Config{RetainFailedCredential: retain, JWTValidator: cfg})
				if err != nil {
					t.Fatal(err)
				}
				probe := &legacyStatusProbe{}
				c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/custom", Name: "records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: inputType, OutputType: reflect.TypeFor[nestedCustomRequiredOutput](), CodecFactory: auth})
				if err != nil {
					t.Fatal(err)
				}
				child := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/orders", Name: "identity"}, Routes: []*spec.Route{{Method: "GET", Path: "/identity"}}}
				childInputType := reflect.TypeFor[nestedOwnerCredentialInput]()
				if scoped {
					childInputType = reflect.TypeFor[scopedRequiredCredentialInput]()
				}
				ca, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: child, InputType: childInputType, OutputType: reflect.TypeFor[nestedOwnerCredentialOutput](), CodecFactory: auth})
				if err != nil {
					t.Fatal(err)
				}
				parentHandler := custom.NewFunc[nestedOwnerInput, nestedCustomRequiredOutput](func(context.Context, *nestedOwnerInput) (*nestedCustomRequiredOutput, error) {
					t.Fatal("rejected parent executed")
					return nil, nil
				})
				if inputType == reflect.TypeFor[optionalParentCredentialInput]() {
					parentHandler = custom.NewFunc[optionalParentCredentialInput, nestedCustomRequiredOutput](func(context.Context, *optionalParentCredentialInput) (*nestedCustomRequiredOutput, error) {
						t.Fatal("rejected parent executed")
						return nil, nil
					})
				}
				childHandler := custom.NewFunc[nestedOwnerCredentialInput, nestedOwnerCredentialOutput](func(context.Context, *nestedOwnerCredentialInput) (*nestedOwnerCredentialOutput, error) {
					t.Fatal("rejected child executed")
					return nil, nil
				})
				if scoped {
					childHandler = custom.New[scopedRequiredCredentialInput, nestedOwnerCredentialOutput](scopedRequiredCredentialContract{t})
				}
				native, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: ca.Component, Input: ca.Input, Output: ca.Output, OutputType: reflect.TypeFor[nestedOwnerCredentialOutput](), Capabilities: rhandler.InvocationCapabilities{Logger: probe}, Handler: childHandler}, {Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[nestedCustomRequiredOutput](), Handler: parentHandler, Capabilities: rhandler.InvocationCapabilities{Logger: probe}}})
				if err != nil {
					t.Fatal(err)
				}
				defer native.Shutdown(t.Context())
				request := httptest.NewRequest("GET", "/records", nil)
				if tc.name != "missing" {
					request.Header.Set("X-Credential", tc.credential)
				}
				result := httptest.NewRecorder()
				gateway.NewHandler(native, nil, "opaque-owner").ServeHTTP(result, request)
				var body struct {
					Status, Message string
				}
				wantMessage := "Unauthorized"
				if tc.name == "missing" {
					wantMessage = "parameter Credential is required"
				}
				if result.Code != 401 || json.Unmarshal(result.Body.Bytes(), &body) != nil || body.Status != "error" || body.Message != wantMessage {
					t.Errorf("custom binding response: status=%d message=%q; want status=401 message=%q", result.Code, body.Message, wantMessage)
				}
				for _, private := range []string{tc.credential, "GENERIC_PRIVATE_SENTINEL", "private-diagnostic", "failed to transform", "failed to parse"} {
					if private != "" && strings.Contains(result.Body.String(), private) {
						t.Error("public response disclosed private verifier data")
					}
				}
				if probe.reads != 0 || probe.init != 0 {
					t.Fatal("actual codec rejection reached SQL/Init")
				}
			})
		}
	}
}
