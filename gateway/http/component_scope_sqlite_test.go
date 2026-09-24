package http

import (
	"context"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/xdatly/response"
)

// scopeContextInput is the server-owned context component's input: the only
// transport value it reads is the caller credential.
type scopeContextInput struct {
	Principal string `parameter:"Principal,kind=header,in=X-Principal"`
}

// scopeContextOutput is the typed decision a consuming component binds.
type scopeContextOutput struct {
	Tenant int `json:"tenant"`
}

// scopedRecordsInput binds the context component natively and derives the
// SQL-bound tenant from that bound output. Neither field is a transport kind.
type scopedRecordsInput struct {
	Scope  *scopeContextOutput `parameter:"Scope,kind=component,in=GET:/scope"`
	Tenant int                 `parameter:"Tenant,kind=param,in=Scope.Tenant,required=true"`
}

// TestHTTPComponentDependencyBindsServerOwnedScopeSQLite proves the native
// Datly v1 pattern: a trusted context component's typed output is bound as a
// component-kind input of the consuming reader, a param-kind derivation carries
// the authorized value into the compiled query, and no transport input of any
// name can supply or override either value.
func TestHTTPComponentDependencyBindsServerOwnedScopeSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tenant INTEGER)", "INSERT INTO records VALUES(11,1),(12,1),(22,2)"); err != nil {
		t.Fatal(err)
	}
	tenants := map[string]int{"alice": 1, "bob": 2}
	contextComponent := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example/scope", Name: "Context"}, Name: "Context",
		Routes: []*spec.Route{{Method: "GET", Path: "/scope"}},
	}
	contextArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: contextComponent, InputType: reflect.TypeFor[scopeContextInput](), OutputType: reflect.TypeFor[scopeContextOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	contextRegistration := &registry.RegisteredComponent{Component: contextArtifact.Component, Input: contextArtifact.Input, OutputType: reflect.TypeFor[scopeContextOutput](), Handler: custom.NewFunc[scopeContextInput, scopeContextOutput](func(_ context.Context, input *scopeContextInput) (*scopeContextOutput, error) {
		tenant, ok := tenants[input.Principal]
		if !ok {
			return nil, &response.Error{Code: 403, Cause: errors.New("unknown principal")}
		}
		return &scopeContextOutput{Tenant: tenant}, nil
	})}

	required := true
	recordsComponent := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example/scope", Name: "Records"}, Name: "Records",
		Routes: []*spec.Route{{Method: "GET", Path: "/api/records"}},
		Parameters: []*spec.Parameter{
			{Name: "Scope", Source: spec.BindSource{Kind: "component", Name: "GET:/scope"}, Required: &required},
			{Name: "Tenant", TypeExpr: "int", Source: spec.BindSource{Kind: "param", Name: "Scope.Tenant"}, Required: &required},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE tenant=:Tenant ORDER BY id"}},
	}
	recordsArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: recordsComponent, InputType: reflect.TypeFor[scopedRecordsInput](), OutputType: reflect.TypeFor[configOutput](), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := recordsArtifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	recordsRegistration := &registry.RegisteredComponent{Component: recordsArtifact.Component, Input: recordsArtifact.Input, OutputType: reflect.TypeFor[configOutput](), Reader: reader}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{contextRegistration, recordsRegistration})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(ctx) })
	handler, err := (Config{}).NewHandler(rt, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	get := func(uri, principal string, headers map[string]string) (int, string) {
		req := httptest.NewRequest("GET", uri, nil)
		if principal != "" {
			req.Header.Set("X-Principal", principal)
		}
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		return res.Code, res.Body.String()
	}

	t.Run("bound context scopes the query", func(t *testing.T) {
		code, body := get("/api/records", "alice", nil)
		if code != stdhttp.StatusOK || !strings.Contains(body, `"id":11`) || !strings.Contains(body, `"id":12`) || strings.Contains(body, `"id":22`) {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("distinct principals receive distinct scope", func(t *testing.T) {
		code, body := get("/api/records", "bob", nil)
		if code != stdhttp.StatusOK || strings.Contains(body, `"id":11`) || !strings.Contains(body, `"id":22`) {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("transport inputs cannot supply or override bound kinds", func(t *testing.T) {
		code, body := get("/api/records?Tenant=2&tenant=2&Scope.Tenant=2&Scope=%7B%22tenant%22%3A2%7D", "alice", map[string]string{"Tenant": "2", "Scope": `{"tenant":2}`})
		if code != stdhttp.StatusOK || strings.Contains(body, `"id":22`) || !strings.Contains(body, `"id":11`) {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("denied context component denies the consumer", func(t *testing.T) {
		code, body := get("/api/records?tenant=1", "mallory", nil)
		if code == stdhttp.StatusOK || strings.Contains(body, `"id":`) {
			t.Fatalf("denied context released data: %d %s", code, body)
		}
	})
	t.Run("missing credential denies", func(t *testing.T) {
		code, body := get("/api/records", "", nil)
		if code == stdhttp.StatusOK || strings.Contains(body, `"id":`) {
			t.Fatalf("missing credential released data: %d %s", code, body)
		}
	})
}
