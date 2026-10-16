package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/data"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/criteria"
	rcollector "github.com/viant/datly/sql/reader/collector"
	sqltemplate "github.com/viant/datly/sql/template"
	xhandler "github.com/viant/xdatly/handler"
	modern "github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/companionpage"
	dexec "github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
)

func pageRuntime(t *testing.T, configure ...func(*bootstrap.Artifact)) *druntime.Runtime {
	t.Helper()
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	if c.Settings == nil || c.Settings.IgnoreEmptyQueryParameters == nil || *c.Settings.IgnoreEmptyQueryParameters {
		t.Fatal("genuine explicit false policy missing")
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range configure {
		f(artifact)
	}
	db := sqlite.New(t)
	if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five'),(6,'six')"); err != nil {
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
func TestDeferredCompanionPageNativeOracle(t *testing.T) {
	rt := pageRuntime(t)
	handler := gateway.NewHandler(rt, nil, "page-companion")
	for _, tc := range []struct {
		name, query string
		wantError   bool
		page        int
	}{
		{"absent", "", false, 0},
		{"custom valid", "?page=2", false, 2},
		{"custom malformed fallback absent", "?page=bad", false, 0},
		{"fallback malformed", "?_page=bad", true, 0},
		{"custom malformed fallback valid", "?page=bad&_page=3", false, 3},
		{"custom valid fallback malformed", "?page=2&_page=bad", false, 2},
		{"repeated custom first valid", "?page=2&page=bad&_page=7", true, 0},
		{"repeated custom first malformed", "?page=bad&page=2&_page=7", true, 0},
		{"repeated fallback first valid", "?_page=3&_page=bad", true, 0},
		{"repeated fallback first malformed", "?_page=bad&_page=3", true, 0},
		{"custom zero", "?page=0&_page=7", false, 1},
		{"custom negative", "?page=-2&_page=7", false, 1},
		{"fallback zero", "?_page=0", false, 1},
		{"fallback negative", "?_page=-2", false, 1},
		{"empty custom fallback valid", "?page=&_page=7", false, 1},
		{"literal comma custom fallback valid", "?page=2,3&_page=7", false, 7},
		{"repeated custom both valid", "?page=2&page=3&_page=7", true, 0},
		{"repeated fallback both valid", "?_page=2&_page=3", true, 0},
		{"empty fallback", "?_page=", false, 1},
		{"custom whitespace fallback valid", "?page=%20&_page=7", false, 7},
		{"custom overflow fallback valid", "?page=999999999999999999999999999999&_page=7", false, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := httptest.NewRecorder()
			handler.ServeHTTP(out, httptest.NewRequest("GET", "/page-records"+tc.query, nil))
			code := 200
			if tc.wantError {
				code = 400
			}
			if out.Code != code {
				scope, _ := requestprovider.New(httptest.NewRequest("GET", "/page-records"+tc.query, nil))
				defer scope.Close()
				target, _ := rt.ComponentTargetByMethodPath("GET", "/page-records")
				_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
				t.Logf("direct cause: %v", cause)
				t.Fatalf("code=%d want%d body=%s", out.Code, code, out.Body.String())
			}
			if tc.wantError {
				if !strings.HasPrefix(out.Header().Get("Content-Type"), "text/plain") {
					t.Fatalf("error media=%s", out.Header().Get("Content-Type"))
				}
				var body struct {
					Status string `json:"status"`
					Errors []struct {
						Parameter, View string
						Code            int `json:"statusCode"`
					} `json:"errors"`
					Data []any `json:"data"`
				}
				if err := json.Unmarshal(out.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if body.Status != "error" || body.Data == nil || len(body.Data) != 0 || len(body.Errors) != 1 || body.Errors[0].Parameter != "Page" || body.Errors[0].Code != 400 {
					t.Fatalf("late Page envelope=%s", out.Body.String())
				}
				scope, _ := requestprovider.New(httptest.NewRequest("GET", "/page-records"+tc.query, nil))
				defer scope.Close()
				target, _ := rt.ComponentTargetByMethodPath("GET", "/page-records")
				_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
				var binding *bindly.BindingError
				if !errors.As(cause, &binding) || binding.Name != "Page" || binding.Kind != "query" || binding.In != "page" {
					t.Fatalf("late native cause=%v", cause)
				}
				return
			}
			var body struct {
				Data []struct {
					ID int `json:"id"`
				} `json:"data"`
			}
			if err := json.Unmarshal(out.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			first := 1
			if tc.page > 1 {
				first = 1 + 2*(tc.page-1)
			}
			if first > 6 {
				if len(body.Data) != 0 {
					t.Fatalf("page%d data=%s", tc.page, out.Body.String())
				}
			} else if len(body.Data) != 2 || body.Data[0].ID != first {
				t.Fatalf("page%d data=%s", tc.page, out.Body.String())
			}
		})
	}
}
func TestDeferredCompanionFieldsPrecedePageNative(t *testing.T) {
	rt := pageRuntime(t)
	handler := gateway.NewHandler(rt, nil, "page-companion")
	for _, query := range []string{"?fields=missing&_page=bad", "?fields=missing&page=bad", "?fields=%20&_page=bad"} {
		t.Run(query, func(t *testing.T) {
			out := httptest.NewRecorder()
			handler.ServeHTTP(out, httptest.NewRequest("GET", "/page-records"+query, nil))
			if out.Code != 400 || !strings.Contains(out.Body.String(), `"parameter":"Fields"`) || strings.Contains(out.Body.String(), `"parameter":"Page"`) {
				t.Fatalf("code%d body=%s", out.Code, out.Body.String())
			}
		})
	}
}

func TestDeferredCompanionExplicitOverrideSkipsMalformedPage(t *testing.T) {
	rt := pageRuntime(t)
	target, _ := rt.ComponentTargetByMethodPath("GET", "/page-records")
	for _, query := range []string{"?fields=missing&page=bad&_page=bad", "?fields=%20&page=2&page=3&_page=bad"} {
		scope, err := requestprovider.New(httptest.NewRequest("GET", "/page-records"+query, nil))
		if err != nil {
			t.Fatal(err)
		}
		providers := append([]locator.Provider(nil), scope.Providers()...)
		providers = append(providers, handlerprovider.Static(xhandler.SelectorsKey, xstate.Selectors{&xstate.NamedSelector{Name: "rows", Selector: xstate.Selector{Fields: []string{"id"}, Page: 2}}}))
		result, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: providers})
		if err != nil {
			t.Fatal(err)
		}
		output, ok := result.(*fixture.Output)
		if !ok || len(output.Data) != 2 || output.Data[0].ID != 3 || output.Data[0].Label != "" {
			t.Fatalf("override Page/Fields not applied to SQL: %#v", result)
		}

		providers[len(providers)-1] = handlerprovider.Static(xhandler.SelectorsKey, xstate.Selectors{&xstate.NamedSelector{Name: "rows", Selector: xstate.Selector{Fields: []string{"missing"}, Page: 1}}})
		_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: providers})
		var binding *bindly.BindingError
		var owner *modern.Error
		var unknown *dsql.UnknownProjectionColumnError
		if errors.As(cause, &binding) || !errors.As(cause, &owner) || !errors.As(cause, &unknown) {
			t.Fatalf("explicit owner/cause changed: %v", cause)
		}
		if _, ok := owner.Payload.(modern.Status); !ok {
			t.Fatal("explicit owner payload replaced")
		}
		scope.Close()
	}
}
func TestDeferredCompanionConcurrentPageAndFieldsAreInvocationLocal(t *testing.T) {
	rt := pageRuntime(t)
	handler := gateway.NewHandler(rt, nil, "page-companion")
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			query := "?fields=id&page=1"
			want := `"id":1`
			forbidden := `"label":`
			if i%2 != 0 {
				query = "?fields=label&page=2"
				want = `"label":"three"`
				forbidden = `"id":`
			}
			out := httptest.NewRecorder()
			handler.ServeHTTP(out, httptest.NewRequest("GET", "/page-records"+query, nil))
			if out.Code != 200 || !strings.Contains(out.Body.String(), want) || strings.Contains(out.Body.String(), forbidden) {
				t.Errorf("request%d code%d body=%s", i, out.Code, out.Body.String())
			}
		}(i)
	}
	wg.Wait()
}

type pageCountingTemplate struct {
	original sqltemplate.Evaluator
	calls    *atomic.Int64
}

func (p *pageCountingTemplate) Evaluate(ctx context.Context, in sqltemplate.Invocation) (sqltemplate.Result, error) {
	p.calls.Add(1)
	return p.original.Evaluate(ctx, in)
}

func TestDeferredCompanionValidatesFieldsAndPageBeforeOneTemplateEvaluation(t *testing.T) {
	var evaluations atomic.Int64
	var plan *reader.Plan
	rt := pageRuntime(t, func(a *bootstrap.Artifact) {
		original := a.Reader
		root := original.Root.View
		route, ok := a.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/page-records"})
		if !ok {
			t.Fatal("actual route missing")
		}
		graph, err := rcollector.Compile(root, map[*data.View]reflect.Type{root: reflect.TypeFor[fixture.Row]()})
		if err != nil {
			t.Fatal(err)
		}
		if original.Root.Template == nil {
			t.Fatal("genuine compiled scalar SQL template missing")
		}
		counted := &pageCountingTemplate{original: original.Root.Template, calls: &evaluations}
		a.Reader, err = reader.NewPlan(reader.PlanConfig{SelectorCompanion: route.SelectorPlan(), SelectorBindings: original.SelectorBindings, ViewIndex: original.ViewIndex, OutputViewField: original.OutputViewField, OutputStatusField: original.OutputStatusField, RootView: root, Templates: map[*data.View]sqltemplate.Evaluator{root: counted}, Criteria: map[*data.View]*criteria.Compiler{root: original.Root.Criteria}, Collection: graph})
		if err != nil {
			t.Fatal(err)
		}
		plan = a.Reader
	})
	handler := gateway.NewHandler(rt, nil, "page-companion")
	for _, tc := range []struct {
		query string
		code  int
		calls int64
	}{
		{"?fields=missing&_page=bad", 400, 0}, {"?fields=%20&_page=bad", 400, 0}, {"?fields=id&_page=bad", 400, 0}, {"?fields=id&page=1", 200, 1},
	} {
		before := evaluations.Load()
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, httptest.NewRequest("GET", "/page-records"+tc.query, nil))
		if out.Code != tc.code || evaluations.Load()-before != tc.calls {
			t.Fatalf("%s code%d evals%d body=%s", tc.query, out.Code, evaluations.Load()-before, out.Body.String())
		}
	}
	// A replacement evaluator cannot borrow a selection bound to the original.
	plan.Root.Template = &pageCountingTemplate{original: plan.Root.Template, calls: &evaluations}
	before := evaluations.Load()
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, httptest.NewRequest("GET", "/page-records?fields=id&page=1", nil))
	if out.Code != 500 || evaluations.Load() != before {
		t.Fatalf("replacement evaluator ran: code%d evaluations%d body%s", out.Code, evaluations.Load()-before, out.Body.String())
	}
}
