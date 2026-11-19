package contract

import (
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	xcontract "github.com/viant/xdatly/contract"
)

type policyInput struct {
	Value string
	Other *policyLeaf
	calls int
}
type policyLeaf struct{ Public string }

var boundCalls atomic.Int64
var fieldCalls atomic.Int64

func (p *policyInput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	p.calls++
	if p.calls != 1 {
		return nil, fmt.Errorf("shared hook receiver")
	}
	var state struct {
		Role string `bind:"kind=header,in=X-Role,required"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	boundCalls.Add(1)
	if state.Role == "admin" {
		return nil, nil
	}
	return &xcontract.Policy{Name: state.Role, Field: func(_ string, _ reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		fieldCalls.Add(1)
		if f.Name != "Value" {
			return nil, nil
		}
		switch state.Role {
		case "hide", "hide2":
			return f.Clone().Hide(), nil
		case "invalid":
			return f.Clone().SetEnum(), nil
		case "noop":
			return f.Clone(), nil
		}
		return nil, fmt.Errorf("unknown role")
	}}, nil
}

func policyBinder(t testing.TB, role string) *bindly.Injector {
	t.Helper()
	root, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/", nil)
	if role != "" {
		req.Header.Set("X-Role", role)
	}
	source, err := request.New(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close() })
	scope, err := root.ForScope(source.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func TestPolicyCacheRetainsTypesNotAuthorization(t *testing.T) {
	boundCalls.Store(0)
	fieldCalls.Store(0)
	p, err := New(reflect.TypeFor[policyInput](), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	hidden, err := p.Resolve(ctx, policyBinder(t, "hide"))
	if err != nil {
		t.Fatal(err)
	}
	if hidden == p.Original() || hidden.Field(0).Tag.Get("json") != "-" || hidden.Field(1).Type != p.Original().Field(1).Type {
		t.Fatal("wrong effective shape")
	}
	count := fieldCalls.Load()
	again, err := p.Resolve(ctx, policyBinder(t, "hide"))
	if err != nil || again != hidden || fieldCalls.Load() != count || boundCalls.Load() != 2 {
		t.Fatal("cache did not isolate binding from rewrite", err)
	}
	if _, err = p.Resolve(ctx, policyBinder(t, "")); err == nil {
		t.Fatal("cache bypassed required caller binding")
	}
	for _, role := range []string{"admin", "noop"} {
		actual, err := p.Resolve(ctx, policyBinder(t, role))
		if err != nil || actual != p.Original() {
			t.Fatal("linked identity", role, err)
		}
	}
	for i := 0; i < 2; i++ {
		if _, err = p.Resolve(ctx, policyBinder(t, "invalid")); err == nil {
			t.Fatal("invalid edit accepted")
		}
	}
	if len(p.variants) > 1 {
		t.Fatal("unbounded cache")
	}
}

func TestPolicyConcurrentCallersAndCancellation(t *testing.T) {
	p, _ := New(reflect.TypeFor[policyInput](), 2)
	admin, restricted := policyBinder(t, "admin"), policyBinder(t, "hide")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(adminRole bool) {
			defer wg.Done()
			b := restricted
			if adminRole {
				b = admin
			}
			actual, err := p.Resolve(context.Background(), b)
			if err != nil {
				t.Error(err)
				return
			}
			if (actual == p.Original()) != adminRole {
				t.Error("caller variant leaked")
			}
		}(i%2 == 0)
	}
	wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Resolve(ctx, admin); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestStaticContractDoesNotRequireBinder(t *testing.T) {
	typ := reflect.TypeFor[policyLeaf]()
	p, err := New(typ, 0)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := p.Resolve(t.Context(), nil)
	if err != nil || actual != typ || p.Dynamic() {
		t.Fatal(actual, err)
	}
}
