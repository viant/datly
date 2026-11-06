package runtime

import (
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xcontract "github.com/viant/xdatly/contract"
)

type contractAuthInput struct {
	Role string `parameter:",kind=header,in=X-Role,required=true"`
}
type contractAuthOutput struct{ Admin bool }
type contractBusinessInput struct {
	Body *struct{ Name string } `parameter:",kind=body,required=true"`
}
type contractBusinessOutput struct {
	Public string
	Secret string
	Nested *contractUnchangedChild
}
type contractUnchangedChild struct{ Value string }

func (*contractBusinessOutput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var access struct {
		Auth  *contractAuthOutput `parameter:",kind=component,in=GET:/contract-auth,required=true"`
		Debug bool                `parameter:",kind=header,in=X-Debug"`
	}
	if err := binder.Bind(ctx, &access); err != nil {
		return nil, err
	}
	if access.Auth.Admin {
		return nil, nil
	}
	return &xcontract.Policy{Name: "restricted", Field: func(_ string, _ reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if f.Name == "Secret" {
			return f.Clone().Hide(), nil
		}
		return nil, nil
	}}, nil
}

func TestResolveContractBindsNativeComponentWithoutBusinessExecution(t *testing.T) {
	var authCalls, businessCalls atomic.Int64
	authArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: componentSpec("ContractAuth", "GET", "/contract-auth", nil), InputType: reflect.TypeFor[contractAuthInput](), OutputType: reflect.TypeFor[contractAuthOutput](),
		Handler: custom.NewFunc(func(_ context.Context, input *contractAuthInput) (*contractAuthOutput, error) {
			authCalls.Add(1)
			if input.Role != "admin" && input.Role != "reader" {
				return nil, fmt.Errorf("denied")
			}
			return &contractAuthOutput{Admin: input.Role == "admin"}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	authRegistration, err := authArtifact.Registration(registry.RegisteredComponent{})
	if err != nil {
		t.Fatal(err)
	}
	businessArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: componentSpec("ContractBusiness", "POST", "/contract-business", nil), InputType: reflect.TypeFor[contractBusinessInput](), OutputType: reflect.TypeFor[contractBusinessOutput](),
		Handler: custom.NewFunc(func(context.Context, *contractBusinessInput) (*contractBusinessOutput, error) {
			businessCalls.Add(1)
			return &contractBusinessOutput{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	business, err := businessArtifact.Registration(registry.RegisteredComponent{})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewRuntime([]*registry.RegisteredComponent{authRegistration, business})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(context.Background())
	target := dexec.ComponentTarget{Component: business.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/contract-business"}}
	for _, role := range []string{"reader", "admin", "reader", "denied", ""} {
		req := httptest.NewRequest("GET", "/metadata", nil)
		if role != "" {
			req.Header.Set("X-Role", role)
		}
		req.Header.Set("X-Debug", "true")
		scope, err := request.New(req)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := rt.ResolveContract(t.Context(), target, scope)
		scope.Close()
		if role == "denied" || role == "" {
			if err == nil || actual != nil {
				t.Fatal("unauthorized metadata returned")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		original := reflect.TypeFor[contractBusinessOutput]()
		if actual.Output != original || role == "admin" && actual.OutputRepresentation != nil {
			t.Fatal("admin linked identity lost")
		}
		if role == "reader" {
			if actual.OutputRepresentation == nil {
				t.Fatal("missing changed output metadata")
			}
			sourceField, _ := original.FieldByName("Secret")
			field := actual.OutputRepresentation.Field(sourceField.Index[0])
			if field.Tag.Get("json") != "-" {
				t.Fatal("policy not applied")
			}
			nested, _ := actual.Output.FieldByName("Nested")
			if nested.Type != reflect.TypeFor[*contractUnchangedChild]() {
				t.Fatal("unchanged subtree replaced")
			}
		}
		if actual.Input != reflect.TypeFor[contractBusinessInput]() {
			t.Fatal("input identity lost")
		}
	}
	if businessCalls.Load() != 0 {
		t.Fatal("metadata executed business handler")
	}
	if authCalls.Load() != 4 {
		t.Fatalf("auth not re-evaluated per request: %d", authCalls.Load())
	}
}
