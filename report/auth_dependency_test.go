package report

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

type authenticatedReportInput struct {
	AccountIDs []int
	Fields     []string
	Auth       *reportAuthorization
}
type reportAuthorization struct{ AccountID int }
type reportAuthInput struct{ Token string }

func TestReportRebindsComponentPredicateDependency(t *testing.T) {
	source := reportSource(t, &spec.ReportSettings{Enabled: true})
	source.Component.Parameters = append(source.Component.Parameters, &spec.Parameter{
		Name: "Auth", Source: spec.BindSource{Kind: "component", Name: "GET:/auth"},
		Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"spend", "account_id"}}},
	})
	compiled, err := handlercompiler.New(handlercompiler.Input{Component: source.Component, InputType: reflect.TypeFor[authenticatedReportInput]()}).Compile()
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
	filters, _ := derived.InputType.FieldByName("Filters")
	if _, exposed := filters.Type.FieldByName("Auth"); exposed {
		t.Fatal("runtime auth dependency exposed as client filter")
	}
	if _, present := filters.Type.FieldByName("AccountIDs"); !present {
		t.Fatal("request predicate omitted")
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: derived.Component, InputType: derived.InputType, OutputType: derived.OutputType, Types: catalog})
	if err != nil {
		t.Fatal(err)
	}
	authComponent := &spec.Component{
		Key:        spec.Key{Kind: spec.KindComponent, Scope: "example.com/auth", Name: "Auth"},
		Routes:     []*spec.Route{{Method: "GET", Path: "/auth"}},
		Parameters: []*spec.Parameter{{Name: "Token", Source: spec.BindSource{Kind: "header", Name: "Authorization"}}},
	}
	authContract, err := handlercompiler.New(handlercompiler.Input{Component: authComponent, InputType: reflect.TypeFor[reportAuthInput]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	authCalls, sourceCalls := 0, 0
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{
		{Component: authComponent, Input: authContract.Input, OutputType: reflect.TypeFor[reportAuthorization](), Handler: customhandler.NewFunc[reportAuthInput, reportAuthorization](func(_ context.Context, input *reportAuthInput) (*reportAuthorization, error) {
			authCalls++
			if input.Token != "valid" {
				return nil, fmt.Errorf("authentication failed")
			}
			return &reportAuthorization{AccountID: 42}, nil
		})},
		{Component: source.Component, Input: source.Input, OutputType: source.OutputType, Handler: customhandler.NewFunc[authenticatedReportInput, reportSourceOutput](func(_ context.Context, input *authenticatedReportInput) (*reportSourceOutput, error) {
			sourceCalls++
			if input.Auth == nil || input.Auth.AccountID != 42 {
				return nil, fmt.Errorf("child auth missing")
			}
			return &reportSourceOutput{Rows: []reportSourceRow{{AccountID: input.Auth.AccountID}}}, nil
		})},
		{Component: artifact.Component, Input: artifact.Input, OutputType: derived.OutputType, Handler: derived.Handler},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, credential := range []string{"valid", "", "invalid"} {
		t.Run("credential_"+credential, func(t *testing.T) {
			input := reflect.New(derived.InputType)
			input.Elem().FieldByName("Dimensions").FieldByName("AccountID").SetBool(true)
			before := sourceCalls
			value, err := rt.InvokeComponent(context.Background(), exec.ComponentRequest{
				Target: exec.ComponentTarget{Component: derived.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/spend/cube"}},
				Input:  input.Interface(),
				Providers: []locator.Provider{handlerprovider.Named("header", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
					return credential, name == "Authorization" && credential != "", nil
				})},
			})
			if credential == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				output, ok := value.(*reportSourceOutput)
				if !ok || len(output.Rows) != 1 || output.Rows[0].AccountID != 42 {
					t.Fatalf("output: %#v", value)
				}
			} else if err == nil || sourceCalls != before {
				t.Fatalf("invalid auth reached source: err=%v calls=%d", err, sourceCalls)
			}
		})
	}
	if authCalls != 3 || sourceCalls != 1 {
		t.Fatalf("auth calls=%d source calls=%d", authCalls, sourceCalls)
	}
}
