package runtime_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
)

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
