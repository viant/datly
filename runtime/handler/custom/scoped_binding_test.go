package custom

import (
	"context"
	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"strings"
	"testing"
)

type scopedPolicyContract struct {
	enabled    bool
	calls      int
	Dependency string `bind:"kind=static"`
}

func (c *scopedPolicyContract) BindsInputByScope() bool { c.calls++; return c.enabled }
func (c *scopedPolicyContract) Exec(context.Context, xhandler.Session, *staticInput, *staticOutput) error {
	return nil
}

type capturePolicyContract struct{ scopedPolicyContract }

func (*capturePolicyContract) CaptureInput(context.Context, *staticInput) (any, error) {
	return nil, nil
}

type initializedScopedInput struct{}

func (*initializedScopedInput) Init(context.Context) error { return nil }

type initializedPolicyContract struct{}

func (initializedPolicyContract) BindsInputByScope() bool { return true }
func (initializedPolicyContract) Exec(context.Context, xhandler.Session, *initializedScopedInput, *staticOutput) error {
	return nil
}

type rawScopedHandler struct{}

func (rawScopedHandler) BindsInputByScope() bool                                   { panic("raw policy must not be called") }
func (rawScopedHandler) Execute(context.Context, rhandler.Invocation) (any, error) { return nil, nil }
func TestScopedPolicyCachedAndRawHandlersNotTrusted(t *testing.T) {
	c := &scopedPolicyContract{enabled: true}
	h := New[staticInput, staticOutput](c)
	for range 3 {
		enabled, err := ScopedInputBinding(h)
		if !enabled || err != nil {
			t.Fatal(enabled, err)
		}
	}
	if c.calls != 1 {
		t.Fatal(c.calls)
	}
	if enabled, err := ScopedInputBinding(rawScopedHandler{}); enabled || err != nil {
		t.Fatal(enabled, err)
	}
	c = &scopedPolicyContract{enabled: false}
	h = New[staticInput, staticOutput](c)
	if enabled, err := ScopedInputBinding(h); enabled || err != nil || c.calls != 1 {
		t.Fatal(enabled, err)
	}
}
func TestScopedPolicyRejectsRealCapturerBeforeStaticBinding(t *testing.T) {
	c := &capturePolicyContract{scopedPolicyContract: scopedPolicyContract{enabled: true}}
	h := New[staticInput, staticOutput](c)
	injector, _ := bindly.NewInjector()
	err := h.(interface {
		BindStatic(context.Context, *bindly.Injector) error
	}).BindStatic(t.Context(), injector)
	if err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatal(err)
	}
	if c.Dependency != "" || c.calls != 1 {
		t.Fatal(c)
	}
	if _, err := Factory[staticInput, staticOutput](func() xhandler.Contract[staticInput, staticOutput] { return c })(); err == nil {
		t.Fatal("factory accepted policy")
	}
	if _, err := ScopedInputBinding(New[initializedScopedInput, staticOutput](initializedPolicyContract{})); err == nil {
		t.Fatal("initializer accepted")
	}
}
