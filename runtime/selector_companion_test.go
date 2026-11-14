package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/xdatly/response"
)

type selectorWindowInput struct {
	JWT    *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	ID     int         `parameter:"ID,kind=query,in=id"`
	Limit  int         `parameter:"Limit,kind=query,in=limit" querySelector:"view=records,property=limit"`
	Offset int         `parameter:"Offset,kind=query,in=offset" querySelector:"view=records,property=offset"`
	Page   int         `parameter:"Page,kind=query,in=page" querySelector:"view=records,property=page"`
}

func (i *selectorWindowInput) Init(context.Context) error {
	if i.ID == 0 {
		return &response.Error{Code: 422, Payload: response.Status{Status: "error", Message: "domain ID missing"}, Cause: errors.New("domain ID missing")}
	}
	return nil
}

func TestNativeSelectorWindowBeforeInit(t *testing.T) {
	ctx := t.Context()
	keys := testharness.NewJWT(t)
	auth, err := runtimeauth.New(ctx, &runtimeauth.Config{JWTValidator: keys.Config()})
	if err != nil {
		t.Fatal(err)
	}
	token := keys.Sign(t, "reader", time.Now().Add(time.Hour))
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"); err != nil {
		t.Fatal(err)
	}
	inputType, outputType := reflect.TypeFor[selectorWindowInput](), reflect.TypeFor[fixture.Output]()
	for _, strict := range []bool{false, true} {
		component := &spec.Component{
			Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com", Name: "Records"},
			Name:   "Records",
			Routes: []*spec.Route{{Method: "POST", Path: "/records"}},
			RootView: &spec.View{
				Name:     "records",
				Source:   &spec.ViewSource{SQL: "SELECT id,label FROM records"},
				Selector: &spec.Selector{RequireLimitForOffset: strict, DefaultLimit: 100, AllowLimit: true, AllowOffset: true, AllowPage: true},
			},
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: outputType, CodecFactory: auth})
		if err != nil {
			t.Fatal(err)
		}
		execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: inputType, OutputType: outputType, Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
		if err != nil {
			t.Fatal(err)
		}
		rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: outputType, Reader: execution}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
		handler := gateway.NewHandler(rt, nil, "selector-window")
		for _, tc := range []struct {
			query, credential string
			code              int
			body              string
		}{
			{"?offset=1", "", 401, ""},
			{"?offset=1", "Bearer invalid", 401, ""},
			{"?offset=1&limit=2", token, 422, "domain ID missing"},
			{"?offset=1&page=1&id=1", token, 200, ""},
			{"?offset=1&limit=2&id=1", token, 200, ""},
		} {
			req := httptest.NewRequest("POST", "/records"+tc.query, nil)
			if tc.credential != "" {
				req.Header.Set("Authorization", tc.credential)
			}
			out := httptest.NewRecorder()
			handler.ServeHTTP(out, req)
			if out.Code != tc.code || !strings.Contains(out.Body.String(), tc.body) {
				t.Fatalf("strict=%v query=%q status=%d body=%s", strict, tc.query, out.Code, out.Body.String())
			}
		}
		req := httptest.NewRequest("POST", "/records?offset=1", nil)
		req.Header.Set("Authorization", token)
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, req)
		if !strict {
			if out.Code != 422 || !strings.Contains(out.Body.String(), "domain ID missing") {
				t.Fatalf("ordinary Init precedence changed: %d %s", out.Code, out.Body.String())
			}
			continue
		}
		var body map[string]any
		if err := json.Unmarshal(out.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		message := "can't use offset without limit - view: records"
		want := map[string]any{"status": "error", "message": message, "errors": []any{map[string]any{"statusCode": float64(400), "message": message}}, "data": []any{}}
		if out.Code != 400 || !reflect.DeepEqual(body, want) {
			t.Fatalf("selector envelope differs: %d %s", out.Code, out.Body.String())
		}
	}
}

func companionRuntime(t *testing.T) *druntime.Runtime {
	t.Helper()
	discovered, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(discovered.Components) != 1 {
		t.Fatal("genuine holder missing")
	}
	component, err := discovered.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
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
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	return rt
}
func TestSelectorCompanionNativeHTTP(t *testing.T) {
	rt := companionRuntime(t)
	handler := gateway.NewHandler(rt, nil, "companion")
	for _, tc := range []struct {
		query string
		code  int
		field string
	}{
		{"", 200, "ID"}, {"?fields=id", 200, "ID"}, {"?fields=label&page=1", 200, "Label"}, {"?fields=missing", 400, ""}, {"?page=bad", 400, ""}, {"?fields=id", 200, "ID"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			out := httptest.NewRecorder()
			handler.ServeHTTP(out, httptest.NewRequest("GET", "/selector-records"+tc.query, nil))
			if out.Code != tc.code {
				scope, err := requestprovider.New(httptest.NewRequest("GET", "/selector-records"+tc.query, nil))
				if err != nil {
					t.Fatal(err)
				}
				defer scope.Close()
				target, _ := rt.ComponentTargetByMethodPath("GET", "/selector-records")
				_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
				t.Logf("genuine native failure: %v", cause)
				t.Fatalf("code=%d want=%d body=%s", out.Code, tc.code, out.Body.String())
			}
			if tc.code == 200 {
				var value map[string]any
				if err := json.Unmarshal(out.Body.Bytes(), &value); err != nil {
					t.Fatal(err)
				}
				data, ok := value["Data"].([]any)
				if !ok || len(data) != 2 {
					t.Fatalf("native records missing: %s", out.Body.String())
				}
				if tc.query != "" {
					if !strings.Contains(out.Body.String(), tc.field) {
						t.Fatalf("selected native field missing: %s", out.Body.String())
					}
				}
			}
		})
	}
}
