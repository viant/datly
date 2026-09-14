package http

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"fmt"
	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	x "github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
	xresponse "github.com/viant/xdatly/response"
)

type warmupJWTInput struct {
	Tenant int
	JWT    *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" predicate:"handler,example.WarmupJWTPolicy" json:"-"`
}

type warmupJWTPolicy struct {
	Input *warmupJWTInput `bind:"kind=input,required"`
}

func (p *warmupJWTPolicy) Compute(_ context.Context, _ any) (*xpredicate.Criteria, error) {
	if p.Input == nil || p.Input.JWT == nil || p.Input.JWT.Subject != "user-one" {
		return nil, &xresponse.Error{Code: 403, Cause: fmt.Errorf("authorization denied")}
	}
	return &xpredicate.Criteria{Expression: "tenant = ?", Placeholders: []any{p.Input.JWT.UserID}}, nil
}

func TestHTTPWarmupVerifiedJWTSQLite(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := runtimeauth.New(context.Background(), &runtimeauth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "test-public-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})}}}})
	if err != nil {
		t.Fatal(err)
	}
	sign := func(subject string, expires time.Time) string {
		value, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": subject, "user_id": 1, "exp": expires.Unix()}).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return "Bearer " + value
	}
	for _, tc := range []struct {
		name, token string
		status      int
	}{
		{"missing", "", 401}, {"invalid", "Bearer invalid.token.signature", 401}, {"expired", sign("user-one", time.Now().Add(-time.Hour)), 401}, {"predicate denied", sign("other-user", time.Now().Add(time.Hour)), 403}, {"verified", sign("user-one", time.Now().Add(time.Hour)), 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newConfigFixture(t)
			f.component.RootView.Source.SQL = `SELECT id FROM records WHERE tenant=:Tenant ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")} ORDER BY id`
			catalog := typecatalog.NewCatalog()
			descriptor := x.NewType(reflect.TypeFor[warmupJWTPolicy](), x.WithPkgPath("example"), x.WithName("WarmupJWTPolicy"))
			if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				t.Fatal(err)
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: f.component, InputType: reflect.TypeFor[warmupJWTInput](), OutputType: reflect.TypeFor[configOutput](), DirectViewField: "Rows", CodecFactory: factory, Types: catalog})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			gate := &warmupGate{Reader: reader, QueryPreparer: reader.(dexec.QueryPreparer), warmer: reader.(dexec.ReaderWarmer), started: make(chan context.Context, 1), release: make(chan struct{})}
			close(gate.release)
			rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[configOutput](), Reader: gate}})
			if err != nil {
				t.Fatal(err)
			}
			h, err := warmupConfig().NewHandler(rt, nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			defer h.Shutdown(context.Background())
			req := httptest.NewRequest("POST", "/admin/warm/records?tenant=2", nil)
			req.Header.Set("Authorization", tc.token)
			req.Header.Set("X-Admin", "admin")
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != tc.status {
				t.Fatalf("%d %s", res.Code, res.Body.String())
			}
			if tc.status != 200 {
				select {
				case <-gate.started:
					t.Fatal("unverified JWT dispatched warmup")
				default:
				}
				return
			}
			if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			read := httptest.NewRequest("GET", "/api/records?tenant=1", nil)
			read.Header.Set("Authorization", tc.token)
			cached := httptest.NewRecorder()
			h.ServeHTTP(cached, read)
			if cached.Code != 200 {
				t.Fatalf("verified cache was not reused: %d %s", cached.Code, cached.Body.String())
			}
			denied := httptest.NewRecorder()
			h.ServeHTTP(denied, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
			if denied.Code != 401 {
				t.Fatalf("warmed data bypassed JWT: %d", denied.Code)
			}
		})
	}
}
