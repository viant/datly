package report

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	rhandler "github.com/viant/datly/runtime/handler"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

type reportBinder struct{ invoker exec.ComponentInvoker }

func (b reportBinder) Bind(context.Context, any) error { return nil }
func (b reportBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key == exec.ComponentInvokerKey {
		return b.invoker, true, nil
	}
	return nil, false, nil
}

type reportInvoker struct {
	request exec.ComponentRequest
	result  any
	err     error
}

func (i *reportInvoker) InvokeComponent(_ context.Context, request exec.ComponentRequest) (any, error) {
	i.request = request
	return i.result, i.err
}

func TestHandlerDelegatesSelectorsAndTypedFilters(t *testing.T) {
	project, _ := generatedProject(t, &spec.ReportSettings{Enabled: true})
	derived := project.Derived()[0]
	input := reflect.New(derived.InputType)
	dimensions := input.Elem().FieldByName("Dimensions")
	dimensions.FieldByName(typecatalog.ExportedFieldName("AccountID")).SetBool(true)
	measures := input.Elem().FieldByName("Measures")
	measures.FieldByName("TotalSpend").SetBool(true)
	filters := input.Elem().FieldByName("Filters")
	accountIDs := []int{3, 5}
	filters.FieldByName(typecatalog.ExportedFieldName("AccountIDs")).Set(reflect.ValueOf(&accountIDs))
	input.Elem().FieldByName("OrderBy").Set(reflect.ValueOf([]string{"AccountID DESC"}))
	limit, offset := 25, 5
	input.Elem().FieldByName("Limit").Set(reflect.ValueOf(&limit))
	input.Elem().FieldByName("Offset").Set(reflect.ValueOf(&offset))

	want := &reportSourceOutput{Rows: []reportSourceRow{{AccountID: 3}}}
	invoker := &reportInvoker{result: want}
	actual, err := derived.Handler.Execute(context.Background(), rhandler.Invocation{
		Input: input.Interface(), Binder: reportBinder{invoker: invoker},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if actual != want || invoker.request.Target != derived.Plan.Target() {
		t.Fatalf("delegation = %#v, %+v", actual, invoker.request.Target)
	}
	selectors := providerValue(t, invoker.request.Providers, string(xhandler.SelectorsKey), reflect.TypeOf(xstate.Selectors{}), "").(xstate.Selectors)
	if len(selectors) != 1 || selectors[0].Name != "spend" || selectors[0].Limit != 25 || selectors[0].Offset != 5 || selectors[0].OrderBy != "AccountID DESC" {
		t.Fatalf("selectors = %+v", selectors)
	}
	wantFields := []string{"AccountID", "Details", "TotalSpend"}
	if !reflect.DeepEqual(selectors[0].Fields, wantFields) {
		t.Fatalf("selector fields = %v, want %v", selectors[0].Fields, wantFields)
	}
	filter := providerValue(t, invoker.request.Providers, "query", reflect.TypeOf([]int{}), "accountID")
	if !reflect.DeepEqual(filter, []int{3, 5}) {
		t.Fatalf("filter = %#v", filter)
	}
}

func TestHandlerOmitsAbsentCollectionFilterAndPreservesExplicitEmpty(t *testing.T) {
	project, _ := generatedProject(t, &spec.ReportSettings{Enabled: true})
	derived := project.Derived()[0]
	execute := func(value *[]int) exec.ComponentRequest {
		input := reflect.New(derived.InputType)
		input.Elem().FieldByName("Dimensions").FieldByName(typecatalog.ExportedFieldName("AccountID")).SetBool(true)
		if value != nil {
			input.Elem().FieldByName("Filters").FieldByName(typecatalog.ExportedFieldName("AccountIDs")).Set(reflect.ValueOf(value))
		}
		invoker := &reportInvoker{}
		if _, err := derived.Handler.Execute(context.Background(), rhandler.Invocation{
			Input: input.Interface(), Binder: reportBinder{invoker: invoker},
		}); err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		return invoker.request
	}

	omitted := execute(nil)
	if _, found := findProviderValue(t, omitted.Providers, "query", reflect.TypeOf([]int{}), "accountID"); found {
		t.Fatal("omitted collection filter published an authoritative provider value")
	}
	if !hasProviderKind(omitted.Providers, "query") {
		t.Fatal("omitted collection filter did not preserve its scoped query provider")
	}
	empty := []int{}
	explicit := execute(&empty)
	value, found := findProviderValue(t, explicit.Providers, "query", reflect.TypeOf([]int{}), "accountID")
	if !found || value == nil || len(value.([]int)) != 0 {
		t.Fatalf("explicit empty collection filter = %#v, found=%v", value, found)
	}
}

func TestHandlerLeavesRequiredFilterFailureToSourceBinding(t *testing.T) {
	settings := &spec.ReportSettings{Enabled: true}
	source := reportSource(t, settings)
	required := true
	source.Component.Parameters[0].Required = &required
	compiled, err := handlercompiler.New(handlercompiler.Input{
		Component: source.Component, InputType: reflect.TypeOf(reportSourceInput{}),
	}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	source.Input = compiled.Input
	catalog := typecatalog.NewCatalog()
	project, err := NewProjectCompiler(ProjectConfig{Types: catalog}).Compile([]Source{source})
	if err != nil {
		t.Fatal(err)
	}
	derived := project.Derived()[0]
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: derived.Component, InputType: derived.InputType, OutputType: derived.OutputType, Types: catalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{
		{
			Component: source.Component, Input: source.Input, OutputType: source.OutputType,
			Handler: customhandler.NewFunc[reportSourceInput, reportSourceOutput](func(_ context.Context, _ *reportSourceInput) (*reportSourceOutput, error) {
				return &reportSourceOutput{}, nil
			}),
		},
		{Component: artifact.Component, Input: artifact.Input, OutputType: derived.OutputType, Handler: derived.Handler},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := reflect.New(derived.InputType)
	input.Elem().FieldByName("Dimensions").FieldByName(typecatalog.ExportedFieldName("AccountID")).SetBool(true)
	_, err = runtime.InvokeComponent(context.Background(), exec.ComponentRequest{
		Target: exec.ComponentTarget{
			Component: derived.Component.Key,
			Route:     spec.RouteRef{Method: derived.Component.Routes[0].Method, Path: derived.Component.Routes[0].Path},
		},
		Input: input.Interface(),
	})
	if err == nil || !strings.Contains(err.Error(), `missing required query value "accountID"`) {
		t.Fatalf("InvokeComponent() error = %v, want required source filter failure", err)
	}
}

func TestHandlerRejectsEmptySelection(t *testing.T) {
	project, _ := generatedProject(t, &spec.ReportSettings{Enabled: true})
	derived := project.Derived()[0]
	_, err := derived.Handler.Execute(context.Background(), rhandler.Invocation{
		Input: reflect.New(derived.InputType).Interface(), Binder: reportBinder{invoker: &reportInvoker{}},
	})
	if err == nil {
		t.Fatal("expected no-selection error")
	}
}

func providerValue(t *testing.T, providers []locator.Provider, kind string, typeOf reflect.Type, name string) any {
	t.Helper()
	value, found := findProviderValue(t, providers, kind, typeOf, name)
	if found {
		return value
	}
	t.Fatalf("provider %s was not found", kind)
	return nil
}

func findProviderValue(t *testing.T, providers []locator.Provider, kind string, typeOf reflect.Type, name string) (any, bool) {
	t.Helper()
	for _, provider := range providers {
		if provider.Kind() != kind {
			continue
		}
		value, ok, err := provider.Locate(nil).Value(context.Background(), typeOf, name)
		if err != nil {
			t.Fatalf("provider %s/%s = %#v, %v, %v", kind, name, value, ok, err)
		}
		return value, ok
	}
	return nil, false
}

func hasProviderKind(providers []locator.Provider, kind string) bool {
	for _, provider := range providers {
		if provider.Kind() == kind {
			return true
		}
	}
	return false
}
