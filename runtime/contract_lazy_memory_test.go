package runtime

import (
	"context"
	"net/http/httptest"
	"reflect"
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

type lazyContractOutput struct{ Secret string }

func (*lazyContractOutput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var access struct {
		Role string `parameter:",kind=header,in=X-Role"`
	}
	if err := binder.Bind(ctx, &access); err != nil {
		return nil, err
	}
	if access.Role == "admin" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "restricted", Field: func(_ string, _ reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		return field.Hide(), nil
	}}, nil
}

func TestLazyContractRetainsOnePlanAcrossRequests(t *testing.T) {
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: componentSpec("LazyContract", "POST", "/lazy-contract", nil),
		InputType: reflect.TypeFor[contractBusinessInput](), OutputType: reflect.TypeFor[lazyContractOutput](),
		Handler: custom.NewFunc(func(context.Context, *contractBusinessInput) (*lazyContractOutput, error) {
			t.Fatal("contract discovery executed business handler")
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	registration, err := artifact.Registration(registry.RegisteredComponent{})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewIndexedRuntime([]*spec.Component{artifact.Component}, nil, constantComponentLoader{registration: registration})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	target := dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/lazy-contract"}}
	var restricted *dexec.EffectiveContract
	for i := 0; i < 32; i++ {
		req := httptest.NewRequest("GET", "/metadata", nil)
		if i%2 == 0 {
			req.Header.Set("X-Role", "admin")
		}
		scope, err := request.New(req)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := rt.ResolveContract(t.Context(), target, scope)
		scope.Close()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rt.contractBindingProof(t.Context(), target.Route); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			if actual.OutputRepresentation != nil {
				t.Fatal("restricted policy leaked into admin request")
			}
		} else {
			if actual.OutputRepresentation == nil || actual.OutputRepresentation.Field(0).Tag.Get("json") != "-" {
				t.Fatal("restricted policy missing")
			}
			if restricted != nil && restricted.OutputRepresentation != actual.OutputRepresentation {
				t.Error("repeated request rebuilt the retained contract representation")
			}
			restricted = actual
		}
	}
	count := 0
	rt.contractPlans.Range(func(_, _ any) bool { count++; return true })
	if count != 1 {
		t.Fatalf("32 requests retained %d contract plans, want 1", count)
	}
}
