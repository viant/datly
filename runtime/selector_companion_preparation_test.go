package runtime_test

import (
	"context"
	"errors"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestSelectorCompanionNativePreparation(t *testing.T) {
	rt := companionRuntime(t)
	target, ok := rt.ComponentTargetByMethodPath("GET", "/selector-records")
	if !ok {
		t.Fatal("target missing")
	}
	for _, query := range []string{"", "?fields=id&page=1", "?fields=label&page=2", "?fields=missing", "?page=bad", "?page=0", "?page=-1"} {
		t.Run(query, func(t *testing.T) {
			for _, prepare := range []bool{false, true} {
				scope, err := requestprovider.New(httptest.NewRequest("GET", "/selector-records"+query, nil))
				if err != nil {
					t.Fatal(err)
				}
				result, callErr := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: scope.Providers(), PrepareQuery: prepare})
				scope.Close()
				t.Logf("prepare=%v result=%T err=%v", prepare, result, callErr)
				invalid := strings.Contains(query, "missing") || strings.Contains(query, "bad") || strings.Contains(query, "-1") && !prepare
				if invalid {
					if callErr == nil {
						t.Fatal("invalid selector admitted")
					}
					continue
				}
				if callErr != nil {
					t.Fatal(callErr)
				}
				if prepare {
					prepared, ok := result.(*dexec.PreparedQuery)
					if !ok {
						t.Fatalf("preparation published ordinary output %T", result)
					}
					if strings.Contains(strings.ToUpper(prepared.SQL), " LIMIT ") || strings.Contains(strings.ToUpper(prepared.SQL), " OFFSET ") {
						t.Fatalf("source window leaked into preparation: %s", prepared.SQL)
					}
				}
			}
		})
	}
}

func TestSelectorCompanionPreparationRejectsNonfreshRequests(t *testing.T) {
	rt := companionRuntime(t)
	target, _ := rt.ComponentTargetByMethodPath("GET", "/selector-records")
	for name, alter := range map[string]func(*dexec.ComponentRequest){
		"bound":       func(r *dexec.ComponentRequest) { r.Input = new(struct{}) },
		"seed":        func(r *dexec.ComponentRequest) { r.ExtraInput = new(dexec.InputSeed) },
		"capture":     func(r *dexec.ComponentRequest) { r.BindingOutput = new(int) },
		"omit":        func(r *dexec.ComponentRequest) { r.WarmupOmitInput = []string{"ID"} },
		"dryrun":      func(r *dexec.ComponentRequest) { r.DryRun = true },
		"independent": func(r *dexec.ComponentRequest) { r.IndependentChildTransactions = true },
	} {
		t.Run(name, func(t *testing.T) {
			request := dexec.ComponentRequest{Target: target, PrepareQuery: true}
			alter(&request)
			if _, err := rt.InvokeComponent(t.Context(), request); err == nil {
				t.Fatal("unsupported preparation admitted")
			}
		})
	}
}

type preparationWindowInput struct {
	Page   int `parameter:"Page,kind=query,in=page" querySelector:"view=rows,property=page"`
	Offset int `parameter:"Offset,kind=query,in=offset" querySelector:"view=rows,property=offset"`
	Limit  int `parameter:"Limit,kind=query,in=limit" querySelector:"view=rows,property=limit"`
}

func TestNativePreparationUnusedWindowControl(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/preparation", Name: "Rows"}, Name: "Rows", Routes: []*spec.Route{{Method: "GET", Path: "/prepare-window"}}, RootView: &spec.View{Name: "rows", Source: &spec.ViewSource{SQL: "SELECT id,label FROM records"}, Selector: &spec.Selector{AllowPage: true, AllowLimit: true, AllowOffset: true, RequireLimitForOffset: true}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[preparationWindowInput](), OutputType: reflect.TypeFor[fixture.Output]()})
	if err != nil {
		t.Fatal(err)
	}
	db := sqlite.New(t)
	if err := db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"); err != nil {
		t.Fatal(err)
	}
	execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[preparationWindowInput](), OutputType: reflect.TypeFor[fixture.Output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(t.Context())
	target, _ := rt.ComponentTargetByMethodPath("GET", "/prepare-window")
	for _, query := range []string{"?page=-1", "?page=bad", "?offset=1"} {
		for _, prepare := range []bool{false, true} {
			scope, err := requestprovider.New(httptest.NewRequest("GET", "/prepare-window"+query, nil))
			if err != nil {
				t.Fatal(err)
			}
			_, err = rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: scope.Providers(), PrepareQuery: prepare})
			scope.Close()
			wantError := query != "?page=-1" || !prepare
			if (err != nil) != wantError {
				t.Fatalf("query=%s prepare=%v err=%v", query, prepare, err)
			}
		}
	}
}

func TestSelectorCompanionPreparationPreservesCancellation(t *testing.T) {
	rt := companionRuntime(t)
	target, _ := rt.ComponentTargetByMethodPath("GET", "/selector-records")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, PrepareQuery: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation cause lost: %T %v", err, err)
	}
}

// Value and named-scope readers already supported native preparation before
// selector-companion admission. Neither can be required to carry new evidence.
type valuePreparationReader struct{}

func (valuePreparationReader) Read(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (any, error) {
	return new(fixture.Output), nil
}
func (valuePreparationReader) PrepareQuery(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (*dexec.PreparedQuery, error) {
	return &dexec.PreparedQuery{SQL: "SELECT 1"}, nil
}

type preparationScopeRow struct {
	Tenant int `sqlx:"tenant"`
}
type namedScopePreparationInput struct {
	Scope []preparationScopeRow `parameter:"Scope,kind=view,in=scope,scope=authorization" view:"scope,type=preparationScopeRow" sql:"SELECT 1 AS tenant"`
}

func TestNoncompanionPreparationPreservesValueReaderAndNamedScope(t *testing.T) {
	for _, inputType := range []reflect.Type{reflect.TypeFor[struct{}](), reflect.TypeFor[namedScopePreparationInput]()} {
		t.Run(inputType.String(), func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/preparation", Name: "Legacy"}, Name: "Legacy", Routes: []*spec.Route{{Method: "GET", Path: "/prepare-legacy"}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: reflect.TypeFor[fixture.Output]()})
			if err != nil {
				t.Fatal(err)
			}
			route, _ := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/prepare-legacy"})
			if inputType == reflect.TypeFor[namedScopePreparationInput]() && !route.HasNamedScopes() {
				t.Fatal("fixture lacks named scope")
			}
			var execution dexec.Reader = valuePreparationReader{}
			if inputType == reflect.TypeFor[namedScopePreparationInput]() {
				execution = &valuePreparationReader{}
			}
			rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution}})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(t.Context())
			target, _ := rt.ComponentTargetByMethodPath("GET", "/prepare-legacy")
			result, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, PrepareQuery: true})
			if err != nil {
				t.Fatal(err)
			}
			if prepared, ok := result.(*dexec.PreparedQuery); !ok || prepared.SQL != "SELECT 1" {
				t.Fatalf("unexpected result: %#v", result)
			}
		})
	}
}
