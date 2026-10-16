package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"fmt"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	rsql "github.com/viant/datly/sql/builder"
	"github.com/viant/datly/sql/reader"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx/testutil/sqlfault"
	xhandler "github.com/viant/xdatly/handler"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

type selectorLegacyInput struct {
	Chosen []string `parameter:",kind=query,in=pick"`
}
type selectorLegacyRow struct {
	ID    int    `sqlx:"id" json:"id"`
	Label string `sqlx:"label" json:"label"`
}
type selectorLegacyMeta struct {
	PageCnt *int `json:"pageCnt"`
	Cnt     int  `json:"cnt"`
}
type selectorLegacyOutput struct {
	Meta selectorLegacyMeta `json:"meta"`
	legacy.Status
	Data []*selectorLegacyRow `json:"data"`
}
type selectorLegacyPointerOutput struct {
	Meta selectorLegacyMeta `json:"meta"`
	*legacy.Status
	Data []*selectorLegacyRow `json:"data"`
}
type selectorModernOutput struct {
	modern.Status
	Data []*selectorLegacyRow `json:"data"`
}

var selectorRowHooks atomic.Int64
var selectorOutputHooks atomic.Int64

func (*selectorLegacyRow) OnFetch(context.Context) error     { selectorRowHooks.Add(1); return nil }
func (*selectorLegacyOutput) Finalize(context.Context) error { selectorOutputHooks.Add(1); return nil }
func (*selectorLegacyPointerOutput) Finalize(context.Context) error {
	selectorOutputHooks.Add(1)
	return nil
}

func selectorRuntime(t *testing.T, output reflect.Type, templates ...sqltemplate.Evaluator) (*druntime.Runtime, *atomic.Int64) {
	t.Helper()
	h := sqlite.New(t)
	if e := h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"); e != nil {
		t.Fatal(e)
	}
	reads := &atomic.Int64{}
	db := h.FaultDB(t, func(_ context.Context, c sqlfault.Call) error {
		if strings.Contains(strings.ToLower(c.SQL), "from records") {
			reads.Add(1)
		}
		return nil
	})
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "selector-owner", Name: "records"}, Settings: &spec.Settings{CaseFormat: "lc"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,label FROM records"}}, Parameters: []*spec.Parameter{
		{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		{Name: "Chosen", Source: spec.BindSource{Kind: "query", Name: "pick"}, QueryListCSV: true, QuerySelector: &spec.QuerySelectorBinding{View: "records", Property: spec.SelectorPropertyFields}},
	}}
	a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[selectorLegacyInput](), OutputType: output})
	if e != nil {
		t.Fatal(e)
	}
	if len(templates) > 0 {
		a.Reader.Root.Template = templates[0]
		if nested, ok := templates[0].(*selectorNestedTemplate); ok {
			nested.view = a.Reader.Root.View
		}
	}
	r, e := reader.NewExecution(reader.Config{Component: a.Component, InputType: reflect.TypeFor[selectorLegacyInput](), OutputType: output, Plan: a.Reader, SQL: &dsql.SQLComponent{DB: db}})
	if e != nil {
		t.Fatal(e)
	}
	rt, e := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: output, Reader: r}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	return rt, reads
}
func TestSelectorLegacyNativeHTTPAndRecovery(t *testing.T) {
	for _, out := range []reflect.Type{reflect.TypeFor[selectorLegacyOutput](), reflect.TypeFor[selectorLegacyPointerOutput]()} {
		t.Run(out.Name(), func(t *testing.T) {
			rt, reads := selectorRuntime(t, out)
			handler := gateway.NewHandler(rt, nil, "selector-owner")
			for _, name := range []string{"missing", "${error}"} {
				before := reads.Load()
				beforeRows, beforeOutput := selectorRowHooks.Load(), selectorOutputHooks.Load()
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest("GET", "/records?pick="+url.QueryEscape(name), nil))
				if rec.Code != 400 || rec.Header().Get("Content-Type") != "text/plain; charset=utf-8" {
					t.Fatalf("status/header differs: %d/%s body=%s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
				}
				var got map[string]any
				if e := json.Unmarshal(rec.Body.Bytes(), &got); e != nil {
					t.Fatal(e)
				}
				var want map[string]any
				_ = json.Unmarshal([]byte(`{"meta":{"pageCnt":null,"cnt":0},"status":"error","message":"Bad Request","errors":[{"view":"records","parameter":"Chosen","statusCode":400,"message":"Bad Request"}],"data":[]}`), &want)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("complete declared source envelope differs: %s", rec.Body.String())
				}
				if reads.Load() != before {
					t.Fatal("invalid projection reached reader SQL")
				}
				if selectorRowHooks.Load() != beforeRows || selectorOutputHooks.Load() != beforeOutput {
					t.Fatal("invalid native projection reached row/success-output hooks")
				}
				target, ok := rt.ComponentTargetByMethodPath("GET", "/records")
				if !ok {
					t.Fatal("target missing")
				}
				provider := handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
					return []string{name}, key == "pick", nil
				})
				_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: []locator.Provider{provider}})
				var binding *bindly.BindingError
				var unknown *dsql.UnknownProjectionColumnError
				if !errors.As(cause, &binding) || binding.Name != "Chosen" || binding.Path != "Chosen" || binding.Kind != "query" || binding.In != "pick" || binding.Code != 400 || binding.Message != "" || !errors.As(cause, &unknown) || unknown.RequestedColumn != name {
					t.Fatalf("native selector owner/cause lost: %T", cause)
				}
				if reads.Load() != before || selectorRowHooks.Load() != beforeRows || selectorOutputHooks.Load() != beforeOutput {
					t.Fatal("direct native selector failure reached SQL or success hooks")
				}
			}
			for _, q := range []string{"", "?pick=id", "?pick=label"} {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest("GET", "/records"+q, nil))
				if rec.Code != 200 {
					t.Fatalf("recovery/valid selector %s: %d %s", q, rec.Code, rec.Body.String())
				}
			}
			if selectorRowHooks.Load() == 0 || selectorOutputHooks.Load() == 0 {
				t.Fatal("valid controls did not reach actual hooks")
			}
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					q, code := "?pick=id", 200
					if i%2 == 0 {
						q, code = "?pick=missing", 400
					}
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, httptest.NewRequest("GET", "/records"+q, nil))
					if rec.Code != code {
						t.Errorf("concurrent owner state leaked: %d want %d", rec.Code, code)
					}
				}(i)
			}
			wg.Wait()
		})
	}
}
func TestSelectorModernNativePresentationUnchanged(t *testing.T) {
	rt, _ := selectorRuntime(t, reflect.TypeFor[selectorModernOutput]())
	rec := httptest.NewRecorder()
	gateway.NewHandler(rt, nil, "modern-selector").ServeHTTP(rec, httptest.NewRequest("GET", "/records?pick=missing", nil))
	if rec.Code != 400 || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatal("modern selector transport changed")
	}
	var got map[string]any
	if e := json.Unmarshal(rec.Body.Bytes(), &got); e != nil {
		t.Fatal(e)
	}
	want := map[string]any{"status": "error", "message": `Fields "missing": unknown field in the declared source projection`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("modern explicit status changed: %s", rec.Body.String())
	}
}

func TestSelectorDeclaredInputCannotOwnCapabilityOverride(t *testing.T) {
	rt, reads := selectorRuntime(t, reflect.TypeFor[selectorLegacyOutput]())
	target, ok := rt.ComponentTargetByMethodPath("GET", "/records")
	if !ok {
		t.Fatal("target missing")
	}
	query := handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
		return []string{"id"}, key == "pick", nil
	})
	provided := xstate.Selectors{&xstate.NamedSelector{Name: "records", Selector: xstate.Selector{Fields: []string{"missing"}}}}
	override := handlerprovider.Static(xhandler.SelectorsKey, provided)
	before := reads.Load()
	_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: []locator.Provider{query, override}})
	var explicit *modern.Error
	var binding *bindly.BindingError
	if !errors.As(cause, &explicit) || explicit.Payload == nil || errors.As(cause, &binding) {
		t.Fatalf("capability override acquired transport owner: %T", cause)
	}
	if _, ok := explicit.Payload.(modern.Status); !ok {
		t.Fatalf("capability override payload changed: %T", explicit.Payload)
	}
	if reads.Load() != before {
		t.Fatal("invalid override reached root SQL")
	}
	if provided[0].Fields[0] != "missing" {
		t.Fatal("caller selector mutated")
	}
}

type selectorForeignTemplate struct{ failure error }

func (e selectorForeignTemplate) Evaluate(context.Context, sqltemplate.Invocation) (sqltemplate.Result, error) {
	return sqltemplate.Result{}, e.failure
}
func TestSelectorForeignEvaluatorUnknownRetainsModernOwner(t *testing.T) {
	native := &dsql.UnknownProjectionColumnError{Column: "private-template-column", RequestedColumn: "private-template-column"}
	rt, reads := selectorRuntime(t, reflect.TypeFor[selectorLegacyOutput](), selectorForeignTemplate{failure: native})
	target, ok := rt.ComponentTargetByMethodPath("GET", "/records")
	if !ok {
		t.Fatal("target missing")
	}
	query := handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
		return []string{"id"}, key == "pick", nil
	})
	before := reads.Load()
	_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: []locator.Provider{query}})
	var binding *bindly.BindingError
	var explicit *modern.Error
	if errors.As(cause, &binding) || !errors.As(cause, &explicit) || explicit.Payload == nil || !errors.Is(cause, native) {
		t.Fatalf("foreign evaluator acquired declared projection owner: %T", cause)
	}
	if reads.Load() != before {
		t.Fatal("foreign evaluator failure reached root SQL")
	}
	rec := httptest.NewRecorder()
	gateway.NewHandler(rt, nil, "foreign-selector").ServeHTTP(rec, httptest.NewRequest("GET", "/records?pick=id", nil))
	if rec.Code != 400 || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("foreign evaluator presentation changed: %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
}

type selectorNestedTemplate struct {
	view      *data.View
	wrapping  string
	lastCause *dsql.UnknownProjectionColumnError
}

func (e *selectorNestedTemplate) Evaluate(ctx context.Context, _ sqltemplate.Invocation) (sqltemplate.Result, error) {
	_, err := rsql.NewBuilder().Build(ctx, rsql.WithBuilderView(e.view), rsql.WithBuilderSQL("SELECT id,label FROM records"), rsql.WithBuilderProjection([]string{"recycled-inner-column"}))
	if !errors.As(err, &e.lastCause) {
		return sqltemplate.Result{}, errors.New("nested native projection control not reached")
	}
	if e.wrapping == "wrapped" {
		err = fmt.Errorf("callback: %w", err)
	}
	if e.wrapping == "joined" {
		err = errors.Join(err, errors.New("private second cause"))
	}
	return sqltemplate.Result{}, err
}
func TestSelectorRecycledSameViewBuildMarkerKeepsPriorOwner(t *testing.T) {
	for _, wrapping := range []string{"direct", "wrapped", "joined"} {
		t.Run(wrapping, func(t *testing.T) {
			evaluator := &selectorNestedTemplate{wrapping: wrapping}
			rt, reads := selectorRuntime(t, reflect.TypeFor[selectorLegacyOutput](), evaluator)
			target, ok := rt.ComponentTargetByMethodPath("GET", "/records")
			if !ok {
				t.Fatal("target missing")
			}
			query := handlerprovider.Named("query", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
				return []string{"id"}, key == "pick", nil
			})
			before := reads.Load()
			_, cause := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: []locator.Provider{query}})
			var binding *bindly.BindingError
			var explicit *modern.Error
			if errors.As(cause, &binding) || !errors.As(cause, &explicit) || explicit.Payload == nil || !errors.Is(cause, evaluator.lastCause) {
				t.Fatalf("recycled same-view marker acquired owner: %T", cause)
			}
			if reads.Load() != before {
				t.Fatal("recycled failure reached SQL")
			}
			rec := httptest.NewRecorder()
			gateway.NewHandler(rt, nil, "recycled-selector").ServeHTTP(rec, httptest.NewRequest("GET", "/records?pick=id", nil))
			if rec.Code != 400 || rec.Header().Get("Content-Type") != "application/json" {
				t.Fatal("recycled marker changed prior modern transport")
			}
		})
	}
}
