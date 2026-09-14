package openapi_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gatewayhttp "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/x"
	xcodec "github.com/viant/xdatly/codec"
	xpredicate "github.com/viant/xdatly/predicate"
)

type jwtProof struct {
	key     *rsa.PrivateKey
	service *auth.Service
}

func newJWTProof(t *testing.T) *jwtProof {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	encoded, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	service, err := auth.New(context.Background(), &auth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "openapi-test-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})}}}})
	require.NoError(t, err)
	return &jwtProof{key: key, service: service}
}
func (p *jwtProof) token(t *testing.T, subject string) string {
	t.Helper()
	token := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": subject, "user_id": 7, "exp": time.Now().Add(time.Hour).Unix()})
	signed, err := token.SignedString(p.key)
	require.NoError(t, err)
	return "Bearer " + signed
}

type claimsInput struct{ Claims *jwt.Claims }

func (p *jwtProof) registration(t *testing.T, required, key bool, factory xcodec.Factory) *registry.RegisteredComponent {
	t.Helper()
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "JWT"}, Routes: []*spec.Route{{Method: "GET", Path: "/jwt"}}, Parameters: []*spec.Parameter{{Name: "Claims", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required, ErrorStatusCode: 401, Codec: &spec.Codec{Body: auth.JwtClaim}}}}
	if key {
		component.Routes[0].APIKeyHeader = "X-API-Key"
		component.Routes[0].APIKeyValue = "key-value"
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[claimsInput](), OutputType: reflect.TypeFor[dependencyResult](), CodecFactory: factory})
	require.NoError(t, err)
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(_ context.Context, inv handler.Invocation) (any, error) {
		input := inv.Input.(*claimsInput)
		if input.Claims == nil {
			return &dependencyResult{Message: "anonymous"}, nil
		}
		return &dependencyResult{Message: input.Claims.Subject}, nil
	})})
	require.NoError(t, err)
	return entry
}

func TestDeclaredVerifiedJWTSecurityAndAPIKeyComposition(t *testing.T) {
	proof := newJWTProof(t)
	valid := proof.token(t, "Ada")
	for _, required := range []bool{false, true} {
		for _, key := range []bool{false, true} {
			t.Run(fmt.Sprintf("required=%v/key=%v", required, key), func(t *testing.T) {
				entry := proof.registration(t, required, key, proof.service)
				contract, ok := entry.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/jwt"})
				require.True(t, ok)
				require.True(t, contract.Fields()[0].VerifiesJWT())
				// Metadata text is no longer authentication authority after compilation.
				entry.Component.Parameters[0].Codec.Body = "renamed-source-only"
				doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
				require.NoError(t, err)
				operation := doc.Paths["/jwt"].Get
				require.Empty(t, operation.Parameters)
				bearer := doc.Components.SecuritySchemes["BearerJWT"]
				require.NotNil(t, bearer)
				require.Equal(t, "http", bearer.Type)
				require.Equal(t, "bearer", bearer.Scheme)
				require.Equal(t, "JWT", bearer.BearerFormat)
				branches := *operation.Security
				if required {
					require.Len(t, branches, 1)
				} else {
					require.Len(t, branches, 2)
					require.NotContains(t, branches[1], "BearerJWT")
				}
				require.Contains(t, branches[0], "BearerJWT")
				for _, branch := range branches {
					apiKeys := 0
					for name := range branch {
						if doc.Components.SecuritySchemes[name].Type == "apiKey" {
							apiKeys++
						}
					}
					require.Equal(t, key, apiKeys == 1)
				}
				rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
				require.NoError(t, err)
				for _, tc := range []struct {
					token  string
					apiKey bool
					ok     bool
				}{
					{valid, true, true}, {"", true, !required}, {"Bearer malformed", true, false}, {valid, false, !key},
				} {
					req := httptest.NewRequest("GET", "/jwt", nil)
					if tc.token != "" {
						req.Header.Set("Authorization", tc.token)
					}
					if tc.apiKey {
						req.Header.Set("X-API-Key", "key-value")
					}
					recorder := httptest.NewRecorder()
					gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
					require.Equal(t, tc.ok, recorder.Code == 200, recorder.Body.String())
				}
			})
		}
	}
}

type unverifiedClaims struct{}

func (unverifiedClaims) New(*xcodec.Config, ...xcodec.Option) (xcodec.Instance, error) {
	return unverifiedClaims{}, nil
}
func (unverifiedClaims) Value(context.Context, any, ...xcodec.Option) (any, error) {
	return &jwt.Claims{}, nil
}

func TestJWTNameAloneIsNotVerifierAuthority(t *testing.T) {
	proof := newJWTProof(t)
	entry := proof.registration(t, true, false, unverifiedClaims{})
	contract, _ := entry.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/jwt"})
	require.False(t, contract.Fields()[0].VerifiesJWT())
	_, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.ErrorContains(t, err, "without runtime policy")
	entry = proof.registration(t, true, true, proof.service)
	entry.Component.Routes[0].APIKeyHeader = "Authorization"
	_, err = (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.ErrorContains(t, err, "share Authorization")
}

type primaryAuthInput struct {
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" predicate:"handler,example.OpenAPIAuthorization" json:"-"`
}
type primaryAuthPredicate struct {
	Input *primaryAuthInput `bind:"kind=input,required"`
}

func (p *primaryAuthPredicate) Compute(_ context.Context, _ any) (*xpredicate.Criteria, error) {
	if p.Input == nil || p.Input.JWT == nil || p.Input.JWT.Subject != "allowed" {
		return nil, fmt.Errorf("authorization denied")
	}
	return &xpredicate.Criteria{Expression: "owner_id = ?", Placeholders: []any{p.Input.JWT.UserID}}, nil
}

func TestDeclaredJWTPrimaryPredicateRouteSQLite(t *testing.T) {
	proof := newJWTProof(t)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE protected_records(id INTEGER,owner_id INTEGER)", "INSERT INTO protected_records VALUES(1,7),(2,8)"))
	types := typecatalog.NewCatalog()
	require.NoError(t, types.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[primaryAuthPredicate](), x.WithName("OpenAPIAuthorization"), x.WithPkgPath("example"))))
	type row struct {
		ID int `json:"id" sqlx:"id"`
	}
	type result struct {
		Data []row `json:"data"`
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Primary"}, Routes: []*spec.Route{{Method: "GET", Path: "/primary"}}, RootView: &spec.View{Name: "Protected", Source: &spec.ViewSource{SQL: `SELECT id FROM protected_records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")} ORDER BY id`}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[primaryAuthInput](), OutputType: reflect.TypeFor[result](), CodecFactory: proof.service, Types: types})
	require.NoError(t, err)
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[primaryAuthInput](), OutputType: reflect.TypeFor[result](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	entry, err := artifact.Registration(registry.RegisteredComponent{Reader: reader})
	require.NoError(t, err)
	doc, err := (openapi.Generator{}).Generate(context.Background(), documentRequest(entry))
	require.NoError(t, err)
	require.Len(t, *doc.Paths["/primary"].Get.Security, 1)
	require.Contains(t, (*doc.Paths["/primary"].Get.Security)[0], "BearerJWT")
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	for _, tc := range []struct {
		token string
		ok    bool
	}{{proof.token(t, "allowed"), true}, {proof.token(t, "denied"), false}, {"", false}, {"Bearer malformed", false}} {
		req := httptest.NewRequest("GET", "/primary", nil)
		if tc.token != "" {
			req.Header.Set("Authorization", tc.token)
		}
		recorder := httptest.NewRecorder()
		gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
		require.Equal(t, tc.ok, recorder.Code == 200, recorder.Body.String())
		if tc.ok {
			require.JSONEq(t, `{"data":[{"id":1}]}`, recorder.Body.String())
		} else {
			require.False(t, strings.Contains(recorder.Body.String(), `"data"`))
		}
	}
}

func TestPrivateDependencyVerifiedJWTIsPartOfParentSecurity(t *testing.T) {
	proof := newJWTProof(t)
	child := proof.registration(t, true, true, proof.service)
	child.Component.Key.Scope = "private"
	type input struct {
		Child *dependencyResult `parameter:"Child,kind=component,in=GET:/jwt"`
	}
	parent := (fixture{input: reflect.TypeFor[input](), output: reflect.TypeFor[dependencyResult](), component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Parent", Scope: "public"}, Routes: []*spec.Route{{Method: "GET", Path: "/parent"}}}, execute: func(_ context.Context, inv handler.Invocation) (any, error) { return inv.Input.(*input).Child, nil }}).registration(t)
	entries := []*registry.RegisteredComponent{parent, child}
	rt, err := druntime.NewRuntime(entries, druntime.WithExposedPackages([]string{"public"}, nil))
	require.NoError(t, err)
	request := documentRequest(entries...)
	request.Visibility = rt
	doc, err := (openapi.Generator{}).Generate(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, doc.Paths, 1)
	require.NotContains(t, doc.Paths, "/jwt")
	require.Len(t, doc.Components.SecuritySchemes, 1, "the child's API-key HTTP gate is not executed")
	require.Contains(t, (*doc.Paths["/parent"].Get.Security)[0], "BearerJWT")
	for _, tc := range []struct {
		token string
		ok    bool
	}{{proof.token(t, "Ada"), true}, {"", false}} {
		req := httptest.NewRequest("GET", "/parent", nil)
		if tc.token != "" {
			req.Header.Set("Authorization", tc.token)
		}
		recorder := httptest.NewRecorder()
		gatewayhttp.NewHandler(rt, nil, "test").ServeHTTP(recorder, req)
		require.Equal(t, tc.ok, recorder.Code == 200, recorder.Body.String())
	}
}
