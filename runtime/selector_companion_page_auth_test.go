package runtime_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/companionpagesecured"
	dexec "github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/sqlx/testutil/sqlfault"
	xhandler "github.com/viant/xdatly/handler"
	modern "github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

func TestDeferredCompanionAuthAndNativeOwnerOrdering(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "test-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})}}}})
	if err != nil {
		t.Fatal(err)
	}
	sign := func(subject string) string {
		value, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": subject, "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return "Bearer " + value
	}
	token, denied := sign("allowed"), sign("denied")
	pkg := reflect.TypeFor[fixture.Component]().PkgPath()
	found, err := bootstrap.ReflectSelectedPackages([]string{pkg}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(found.Components) != 1 {
		t.Fatal("actual holder not discovered")
	}
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), CodecFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	authFailure := errors.New("native auth fixture denial")
	var authCalls, reads atomic.Int64
	authHandler := custom.NewFunc(func(_ context.Context, input *fixture.AuthInput) (*fixture.AuthOutput, error) {
		authCalls.Add(1)
		if input.JWT.Subject == "denied" {
			return nil, &modern.Error{Code: 403, Payload: modern.Status{Status: "error", Message: "Forbidden"}, Cause: authFailure}
		}
		return &fixture.AuthOutput{Allowed: true}, nil
	})
	authComponent := &spec.Component{Name: "Auth", Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Auth"}, Routes: []*spec.Route{{Method: "GET", Path: "/companion-auth", Handler: "fixture.Auth"}}}
	authArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: authComponent, InputType: reflect.TypeFor[fixture.AuthInput](), OutputType: reflect.TypeFor[fixture.AuthOutput](), Handler: authHandler, CodecFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	db := sqlite.New(t)
	if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"); err != nil {
		t.Fatal(err)
	}
	counted := db.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if strings.Contains(strings.ToLower(call.SQL), "from records") {
			reads.Add(1)
		}
		return nil
	})
	execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: counted}})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{
		{Component: authArtifact.Component, Input: authArtifact.Input, Output: authArtifact.Output, OutputType: reflect.TypeFor[fixture.AuthOutput](), Handler: authHandler},
		{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	handler := gateway.NewHandler(rt, nil, pkg)
	for _, tc := range []struct {
		name, credential, query string
		code                    int
		auth                    int64
	}{
		{"missing JWT before malformed selector", "", "?_page=bad", 401, 0},
		{"malformed JWT before malformed selector", "Bearer invalid.token.signature", "?_page=bad", 401, 0},
		{"Auth denial before malformed selector", denied, "?_page=bad", 403, 1},
		{"late Page after successful Auth", token, "?_page=bad", 400, 1},
		{"projection with nominal owner", token, "?fields=missing&_page=bad", 400, 1},
		{"absent selectors", token, "?id=5", 200, 1},
		{"selected recovery", token, "?fields=id&page=1&id=5", 200, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beforeAuth, beforeReads := authCalls.Load(), reads.Load()
			request := httptest.NewRequest("GET", "/companion-secured"+tc.query, nil)
			if tc.credential != "" {
				request.Header.Set("Authorization", tc.credential)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, request)
			if rec.Code != tc.code || authCalls.Load()-beforeAuth != tc.auth {
				t.Fatalf("ordering/status=%d auth=%d body=%s", rec.Code, authCalls.Load()-beforeAuth, rec.Body.String())
			}
			if tc.code != 200 && reads.Load() != beforeReads {
				t.Fatal("failure reached protected SQL")
			}
			if tc.name == "projection with nominal owner" {
				want := `{"status":"error","message":"Bad Request","errors":[{"view":"secured","parameter":"Fields","statusCode":400,"message":"Bad Request"}],"data":[]}`
				var actual, expected any
				if err := json.Unmarshal(rec.Body.Bytes(), &actual); err != nil {
					t.Fatal(err)
				}
				_ = json.Unmarshal([]byte(want), &expected)
				if rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" || !reflect.DeepEqual(actual, expected) {
					t.Fatalf("complete nominal source envelope changed: %s", rec.Body.String())
				}
			}
			if tc.name == "selected recovery" {
				if !strings.Contains(rec.Body.String(), `"id":1`) || strings.Contains(rec.Body.String(), "label") {
					t.Fatalf("native selected body: %s", rec.Body.String())
				}
			}
		})
	}
	// A direct native call retains the actual projection cause and canonical owner.
	target, _ := rt.ComponentTargetByMethodPath("GET", "/companion-secured")
	// Use separate native request providers for the direct call, with the same header.
	req := httptest.NewRequest("GET", "/companion-secured?fields=missing", nil)
	req.Header.Set("Authorization", token)
	direct, err := requestprovider.New(req)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: direct.Providers()})
	var binding *bindly.BindingError
	var unknown *dsql.UnknownProjectionColumnError
	if !errors.As(cause, &binding) || binding.Name != "Fields" || binding.Path != "Fields" || binding.Kind != "query" || binding.In != "fields" || binding.Code != 400 || !errors.As(cause, &unknown) || unknown.RequestedColumn != "missing" {
		t.Fatalf("canonical native owner/cause missing: %T", cause)
	}
	// An explicit selector capability overrides query selectors and clears authored ownership.
	overridden := append([]locator.Provider(nil), direct.Providers()...)
	overridden = append(overridden, handlerprovider.Static(xhandler.SelectorsKey, xstate.Selectors{&xstate.NamedSelector{Name: "secured", Selector: xstate.Selector{Fields: []string{"id"}}}}))
	if _, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: overridden}); err != nil {
		t.Fatal(err)
	}
	// Invalid explicit selectors retain modern explicit-body precedence and
	// cannot borrow the declaration-owned legacy envelope.
	invalidOverride := append([]locator.Provider(nil), direct.Providers()...)
	invalidOverride = append(invalidOverride, handlerprovider.Static(xhandler.SelectorsKey, xstate.Selectors{&xstate.NamedSelector{Name: "secured", Selector: xstate.Selector{Fields: []string{"missing"}}}}))
	_, overrideError := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: invalidOverride})
	var modernOwner *modern.Error
	binding = nil
	if errors.As(overrideError, &binding) || !errors.As(overrideError, &modernOwner) || !errors.As(overrideError, &unknown) {
		t.Fatalf("explicit selector owner was rewritten: %T", overrideError)
	}
	if _, ok := modernOwner.Payload.(modern.Status); !ok {
		t.Fatal("explicit modern selector payload replaced")
	}
}
