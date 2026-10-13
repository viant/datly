package compiler

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/bindly"
	"github.com/viant/datly/internal/testharness"
	mcptool "github.com/viant/datly/mcp/tool"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/spec"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSharedHeaderBindsVerifiedClaimsAndOriginalToken(t *testing.T) {
	type input struct {
		Claims   *jwt.Claims
		RawToken string
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	service, err := runtimeauth.New(context.Background(), &runtimeauth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "test-public", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})}}}})
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/forward"}}, Parameters: []*spec.Parameter{
		{Name: "Claims", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, TypeExpr: "string", Codec: &spec.Codec{Body: runtimeauth.JwtClaim}},
		{Name: "RawToken", Source: spec.BindSource{Kind: "header", Name: "authorization"}},
	}}
	compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[input](), CodecFactory: service}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/forward"})
	if !ok {
		t.Fatal("route missing")
	}
	tool, err := mcptool.NewCompiler(mcptool.WithStrictArguments(true)).Compile(mcptool.Input{Component: spec.Key{Kind: spec.KindComponent, Name: "Forward"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "Forward"}, Contract: route})
	if err != nil {
		t.Fatal(err)
	}
	if len(tool.Metadata().InputSchema.Properties) != 0 {
		t.Fatalf("header fields leaked into public MCP schema: %+v", tool.Metadata().InputSchema.Properties)
	}
	if _, err := tool.Scope(map[string]interface{}{"RawToken": "client-token"}); err == nil {
		t.Fatal("MCP client supplied hidden header destination")
	}
	token, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "verified-user", "user_id": 17, "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, token string
		valid       bool
	}{{"signed token", token, true}, {"invalid token", "invalid-token", false}} {
		t.Run(tc.name, func(t *testing.T) {
			header := "Bearer " + tc.token
			scope := testharness.NewRequest("POST", "/forward").WithHeaders(http.Header{"Authorization": {header}})
			scoped, err := injector.ForScope(scope.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			actual := &input{}
			err = scoped.Bind(context.Background(), actual, bindly.WithPlan(route.Plan()))
			if !tc.valid {
				if err == nil {
					t.Fatal("invalid JWT accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if actual.Claims == nil || actual.Claims.Subject != "verified-user" || actual.Claims.UserID != 17 || actual.RawToken != header {
				t.Fatalf("header destinations differ: claims=%+v raw matches=%v", actual.Claims, actual.RawToken == header)
			}
		})
	}
}
func TestSharedHeaderStillRejectsAmbiguousCanonicalVariable(t *testing.T) {
	type input struct {
		First  string `bind:"Value,kind=header,in=Authorization"`
		Second string `bind:"Value,kind=header,in=Authorization"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Value", Source: spec.BindSource{Kind: "header", Name: "Authorization"}}}}
	if _, err := BuildBindingSpecs(component, reflect.TypeFor[input](), nil); err == nil || !strings.Contains(err.Error(), "matches multiple input fields") {
		t.Fatalf("ambiguous canonical variable error=%v", err)
	}
}
