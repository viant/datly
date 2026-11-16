package runtime_test

import (
	"context"
	"errors"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/companionsecured"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/xdatly/response"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSelectorCompanionPreparationPreservesJWTAuthAndInit(t *testing.T) {
	keys := testharness.NewJWT(t)
	factory, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: keys.Config()})
	if err != nil {
		t.Fatal(err)
	}
	pkg := reflect.TypeFor[fixture.Component]().PkgPath()
	found, err := bootstrap.ReflectSelectedPackages([]string{pkg}, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), CodecFactory: factory})
	if err != nil {
		t.Fatal(err)
	}
	denial := errors.New("preparation auth fixture denied")
	authCalls := 0
	authHandler := custom.NewFunc(func(_ context.Context, input *fixture.AuthInput) (*fixture.AuthOutput, error) {
		authCalls++
		if input.JWT.Subject == "denied" {
			return nil, &response.Error{Code: 403, Payload: response.Status{Status: "error", Message: "Forbidden"}, Cause: denial}
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
	execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: authArtifact.Component, Input: authArtifact.Input, Output: authArtifact.Output, OutputType: reflect.TypeFor[fixture.AuthOutput](), Handler: authHandler}, {Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(t.Context())
	fixture.Inputs = make(chan *fixture.Input, 32)
	defer func() { fixture.Inputs = nil }()
	target, _ := rt.ComponentTargetByMethodPath("GET", "/companion-secured")
	allowed := keys.Sign(t, "allowed", time.Now().Add(time.Hour))
	denied := keys.Sign(t, "denied", time.Now().Add(time.Hour))
	for _, tc := range []struct {
		credential, query  string
		status, auth, init int
	}{
		{"", "?page=bad", 401, 0, 0}, {"Bearer invalid", "?page=bad", 401, 0, 0}, {denied, "?page=bad", 403, 1, 0}, {allowed, "?page=bad", 400, 1, 0}, {allowed, "?id=-1", 500, 1, 1}, {allowed, "?fields=id&page=1", 0, 1, 1},
	} {
		for _, prepare := range []bool{false, true} {
			beforeAuth, beforeInit := authCalls, len(fixture.Inputs)
			req := httptest.NewRequest("GET", "/companion-secured"+tc.query, nil)
			if tc.credential != "" {
				req.Header.Set("Authorization", tc.credential)
			}
			providers, err := requestprovider.New(req)
			if err != nil {
				t.Fatal(err)
			}
			result, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: providers.Providers(), PrepareQuery: prepare})
			providers.Close()
			if tc.status == 0 {
				if cause != nil {
					t.Fatal(cause)
				}
				if prepare {
					if _, ok := result.(*dexec.PreparedQuery); !ok {
						t.Fatalf("ordinary output published: %T", result)
					}
				}
			} else {
				if cause == nil {
					t.Fatal("failure admitted")
				}
				if tc.status != 500 && response.ErrorStatusCode(cause, 500) != tc.status {
					t.Fatalf("query=%s prepare=%v status=%d cause=%v", tc.query, prepare, response.ErrorStatusCode(cause, 500), cause)
				}
			}
			if authCalls-beforeAuth != tc.auth || len(fixture.Inputs)-beforeInit != tc.init {
				t.Fatalf("query=%s prepare=%v auth=%d init=%d", tc.query, prepare, authCalls-beforeAuth, len(fixture.Inputs)-beforeInit)
			}
			if tc.status == 403 && !errors.Is(cause, denial) {
				t.Fatalf("auth cause lost: %v", cause)
			}
			if tc.status == 500 && !strings.Contains(cause.Error(), "fixture input initialization failed") {
				t.Fatalf("init cause lost: %v", cause)
			}
		}
	}
}
