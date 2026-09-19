package http

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	x "github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
	xresponse "github.com/viant/xdatly/response"
)

type warmupChildrenInput struct {
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" predicate:"handler,example.WarmupJWTPolicy" json:"-"`
}

type warmupChildrenPolicy struct {
	Input *warmupChildrenInput `bind:"kind=input,required"`
}

func (p *warmupChildrenPolicy) Compute(_ context.Context, _ any) (*xpredicate.Criteria, error) {
	if p.Input == nil || p.Input.JWT == nil || p.Input.JWT.Subject != "user-one" {
		return nil, &xresponse.Error{Code: 403, Cause: errors.New("authorization denied")}
	}
	return &xpredicate.Criteria{Expression: "tenant = ?", Placeholders: []any{p.Input.JWT.UserID}}, nil
}

type warmupChildRow struct {
	ID       int    `sqlx:"id" json:"id"`
	ParentID int    `sqlx:"parent_id" json:"parentId"`
	Name     string `sqlx:"name" json:"name"`
}

type warmupParentRow struct {
	ID        int              `sqlx:"id" json:"id"`
	Tenant    int              `sqlx:"tenant" json:"tenant"`
	ChildrenA []warmupChildRow `sqlx:"-" json:"childrenA"`
	ChildrenB []warmupChildRow `sqlx:"-" json:"childrenB"`
}

type warmupChildrenOutput struct {
	Rows []warmupParentRow `json:"rows"`
}

func TestHTTPWarmupExecutesChildViewTargetsSQLite(t *testing.T) {
	ctx := context.Background()
	f := newConfigFixture(t)
	if err := f.db.ExecStatements(ctx,
		"DROP TABLE records",
		"CREATE TABLE parents(id INTEGER, tenant INTEGER)",
		"CREATE TABLE child_a(id INTEGER, parent_id INTEGER, name TEXT)",
		"CREATE TABLE child_b(id INTEGER, parent_id INTEGER, name TEXT)",
		"INSERT INTO parents VALUES(1,1),(2,2)",
		"INSERT INTO child_a VALUES(11,1,'a-one'),(21,2,'a-two')",
		"INSERT INTO child_b VALUES(12,1,'b-one'),(22,2,'b-two')",
	); err != nil {
		t.Fatal(err)
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := runtimeauth.New(ctx, &runtimeauth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "test-public-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})}}}})
	if err != nil {
		t.Fatal(err)
	}
	sign := func(subject string, userID int) string {
		value, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": subject, "user_id": userID, "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return "Bearer " + value
	}
	token := sign("user-one", 1)

	childA := &spec.View{Name: "childA", Source: &spec.ViewSource{
		Bindings: &spec.ViewBindings{CacheName: "child-a-cache", CacheWarmup: "child-a-warmup"},
		SQL:      "SELECT id,parent_id,name FROM child_a WHERE $COLUMN_IN ORDER BY parent_id,id",
	}}
	childB := &spec.View{Name: "childB", Source: &spec.ViewSource{
		Bindings: &spec.ViewBindings{CacheName: "child-b-cache", CacheWarmup: "child-b-warmup"},
		SQL:      "SELECT id,parent_id,name FROM child_b WHERE $COLUMN_IN ORDER BY parent_id,id",
	}}
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Name: "Parents", Scope: "example/public"},
		Routes: []*spec.Route{{Method: "GET", Path: "/api/parents"}},
		Parameters: []*spec.Parameter{
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{
			Name:   "parents",
			Source: &spec.ViewSource{SQL: `SELECT id,tenant FROM parents ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")} ORDER BY id`},
			Relations: []*spec.Relation{
				{Name: "childrenA", Holder: "ChildrenA", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: childA},
				{Name: "childrenB", Holder: "ChildrenB", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: childB},
			},
		},
	}
	catalog := typecatalog.NewCatalog()
	descriptor := x.NewType(reflect.TypeFor[warmupChildrenPolicy](), x.WithPkgPath("example"), x.WithName("WarmupJWTPolicy"))
	if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[warmupChildrenInput](), OutputType: reflect.TypeFor[warmupChildrenOutput](), DirectViewField: "Rows", CodecFactory: factory, Types: catalog})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{
		SQL: &dsql.SQLComponent{DB: f.db.DB},
		CacheSettings: map[string]*spec.CacheSettings{
			"child-a-cache": {Enabled: true, Location: t.TempDir(), TTL: "1m"},
			"child-b-cache": {Enabled: true, Location: t.TempDir(), TTL: "1m"},
			"child-a-warmup": {Warmup: &spec.CacheWarmupSettings{
				IndexColumn: "parent_id",
			}},
			"child-b-warmup": {Warmup: &spec.CacheWarmupSettings{
				IndexColumn: "parent_id",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[warmupChildrenOutput](), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	config := warmupConfig()
	var warmupErr error
	config.Warmup.Authorize = func(_ context.Context, req *stdhttp.Request, target dexec.ComponentTarget) error {
		if req.Header.Get("X-Admin") != "admin" || target.Route.Path != "/api/parents" {
			return errors.New("denied")
		}
		return nil
	}
	config.Warmup.Completed = func(_ WarmupResult, err error) { warmupErr = err }
	h, err := config.NewHandler(rt, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown(ctx)

	missing := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/admin/warm/parents", nil)
	req.Header.Set("X-Admin", "admin")
	h.ServeHTTP(missing, req)
	if missing.Code != 401 {
		t.Fatalf("missing credential status=%d body=%s", missing.Code, missing.Body.String())
	}
	forbidden := httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/admin/warm/parents", nil)
	req.Header.Set("Authorization", token)
	h.ServeHTTP(forbidden, req)
	if forbidden.Code != 403 {
		t.Fatalf("missing admin status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}
	warmed := httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/admin/warm/parents", nil)
	req.Header.Set("Authorization", token)
	req.Header.Set("X-Admin", "admin")
	h.ServeHTTP(warmed, req)
	if warmed.Code != 200 {
		t.Fatalf("warmup status=%d body=%s error=%v", warmed.Code, warmed.Body.String(), warmupErr)
	}
	if err := f.db.ExecStatements(ctx,
		"UPDATE child_a SET name='a-changed'",
		"UPDATE child_b SET name='b-changed'",
	); err != nil {
		t.Fatal(err)
	}
	read := httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/parents", nil)
	req.Header.Set("Authorization", token)
	h.ServeHTTP(read, req)
	if read.Code != 200 {
		t.Fatalf("read status=%d body=%s", read.Code, read.Body.String())
	}
	var actual warmupChildrenOutput
	if err := json.Unmarshal(read.Body.Bytes(), &actual); err != nil {
		t.Fatal(err)
	}
	if len(actual.Rows) != 1 || actual.Rows[0].ID != 1 || actual.Rows[0].Tenant != 1 {
		t.Fatalf("ACL did not restrict parent rows: %+v", actual.Rows)
	}
	if len(actual.Rows[0].ChildrenA) != 1 || actual.Rows[0].ChildrenA[0].Name != "a-one" ||
		len(actual.Rows[0].ChildrenB) != 1 || actual.Rows[0].ChildrenB[0].Name != "b-one" {
		t.Fatalf("child warmup not reused: %+v", actual.Rows[0])
	}
}
