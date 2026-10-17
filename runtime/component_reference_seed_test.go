package runtime

import (
	"context"
	"fmt"
	dexec "github.com/viant/datly/exec"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

type referenceAuth struct {
	roles  map[string]bool
	hidden any
}
type referenceMarkers struct{ Auth, Rows bool }
type referenceInput struct {
	Auth *referenceAuth    `parameter:"Auth,kind=component,in=GET:/reference-auth"`
	Rows []int             `parameter:"Rows,kind=query,in=rows"`
	Has  *referenceMarkers `setMarker:"true"`
}

func (i *referenceInput) Init(context.Context) error {
	if i.Auth == nil || !i.Auth.roles["reader"] {
		return fmt.Errorf("authorization predicate denied")
	}
	return nil
}

type referenceOutput struct {
	Input     *referenceInput
	Finalized bool
}

func (o *referenceOutput) Finalize(context.Context) error { o.Finalized = true; return nil }

type referenceReader struct{}

func (*referenceReader) Read(_ context.Context, input any, _ xhandler.Binder, _ sqlx.ParameterResolver) (any, error) {
	return &referenceOutput{Input: input.(*referenceInput)}, nil
}
func referenceFixture(t *testing.T) (*Runtime, dexec.ComponentTarget, *atomic.Int32) {
	t.Helper()
	auth := componentArtifact(t, componentSpec("ReferenceAuth", "GET", "/reference-auth", nil), reflect.TypeFor[componentGuardInput](), reflect.TypeFor[referenceAuth]())
	reader := componentArtifact(t, componentSpec("ReferenceReader", "GET", "/reference-reader", nil), reflect.TypeFor[referenceInput](), reflect.TypeFor[referenceOutput]())
	calls := new(atomic.Int32)
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: auth.Component, Input: auth.Input, OutputType: reflect.TypeFor[referenceAuth](), Handler: customhandler.NewFunc[componentGuardInput, referenceAuth](func(context.Context, *componentGuardInput) (*referenceAuth, error) {
		calls.Add(1)
		return &referenceAuth{roles: map[string]bool{"reader": true}}, nil
	})}, {Component: reader.Component, Input: reader.Input, OutputType: reflect.TypeFor[referenceOutput](), Reader: &referenceReader{}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	return rt, dexec.ComponentTarget{Component: reader.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/reference-reader"}}, calls
}
func TestReaderSeedComponentReferencePrivateDataAndLifecycle(t *testing.T) {
	rt, target, calls := referenceFixture(t)
	auth := &referenceAuth{roles: map[string]bool{"reader": true}}
	seed := &referenceInput{Auth: auth, Rows: []int{7}, Has: &referenceMarkers{Auth: true, Rows: true}}
	var wg sync.WaitGroup
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(seed)})
			if err != nil {
				t.Error(err)
				return
			}
			out := v.(*referenceOutput)
			if out.Input.Auth != auth || !out.Finalized || !out.Input.Has.Auth {
				t.Error("reference or lifecycle lost")
			}
			out.Input.Rows[0] = 9
			if seed.Rows[0] != 7 {
				t.Error("ordinary seed aliases caller")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 0 {
		t.Fatal("explicit component dependency reexecuted")
	}
	other := &referenceAuth{roles: map[string]bool{"reader": true}}
	v, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(&referenceInput{Auth: other, Has: &referenceMarkers{Auth: true}})})
	if err != nil || v.(*referenceOutput).Input.Auth != other {
		t.Fatalf("cross invocation leak: %v", err)
	}
	_, err = rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(&referenceInput{Auth: &referenceAuth{roles: map[string]bool{}}, Has: &referenceMarkers{Auth: true}})})
	if err == nil {
		t.Fatal("canonical Init authorization check bypassed")
	}
}
func TestReaderSeedBorrowRejectsHiddenCapabilities(t *testing.T) {
	rt, target, calls := referenceFixture(t)
	for _, hidden := range []any{make(chan int), func() {}, context.Background(), &sync.Mutex{}, &Runtime{}} {
		auth := &referenceAuth{roles: map[string]bool{"reader": true}, hidden: hidden}
		_, err := rt.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: target, ExtraInput: dexec.WithInput(&referenceInput{Auth: auth, Has: &referenceMarkers{Auth: true}})})
		if err == nil {
			t.Errorf("hidden capability admitted: %T", hidden)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("dependency executed after rejected reference")
	}
}
