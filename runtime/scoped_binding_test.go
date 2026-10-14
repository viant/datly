package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
)

type scopedNativeInput struct {
	Credential *string                           `parameter:",kind=header,in=Authorization,required=true,errorCode=401"`
	Child      *scopedNativeChildOutput          `parameter:",kind=component,in=GET:/scope-child,scope=lazy"`
	Has        *struct{ Credential, Child bool } `setMarker:"true"`
}
type scopedNativeOutput struct{ Value string }
type scopedNativeChildInput struct{}
type scopedNativeChildOutput struct{ Value string }
type scopedNativeContract struct{}

func (scopedNativeContract) BindsInputByScope() bool { return true }
func (scopedNativeContract) Exec(ctx context.Context, s xhandler.Session, input *scopedNativeInput, out *scopedNativeOutput) error {
	log := xlogger.FromContext(ctx)
	if log != nil {
		log.Info("begin")
	}
	if input.Credential == nil || input.Child != nil || input.Has == nil || !input.Has.Credential || input.Has.Child {
		return errors.New("invalid default prebinding")
	}
	b := s.Binder().(xhandler.ScopedBinder)
	original, found, err := b.Lookup(ctx, xhandler.InputKey)
	if err != nil || !found || original != input {
		return errors.New("canonical input lost")
	}
	if input.Credential == nil || input.Has == nil || !input.Has.Credential || input.Has.Child {
		return errors.New("invalid default markers")
	}
	marker := input.Has
	if *input.Credential == "deny" {
		return errors.New("denied")
	}
	if log != nil {
		log.Info("authorize")
	}
	if err := b.BindScope(ctx, input, "lazy"); err != nil {
		return err
	}
	if input.Has != marker || !input.Has.Child || input.Child == nil {
		return errors.New("invalid child marker")
	}
	out.Value = input.Child.Value
	if log != nil {
		log.Info("end")
	}
	return nil
}

type scopedNativeLog struct {
	mu     sync.Mutex
	events []string
}

func (l *scopedNativeLog) Debug(string, ...any) {}
func (l *scopedNativeLog) Info(msg string, _ ...any) {
	l.mu.Lock()
	l.events = append(l.events, msg)
	l.mu.Unlock()
}
func (l *scopedNativeLog) Warn(string, ...any)  {}
func (l *scopedNativeLog) Error(string, ...any) {}
func TestScopedNativeComposedBindingAndDispatcherAdmission(t *testing.T) {
	sentinel := errors.New("actual child source")
	for _, tc := range []struct {
		name, credential, want string
		childFail              bool
	}{{"success", "ok", "default,begin,authorize,child,end", false}, {"missing", "", "default", false}, {"denied", "deny", "default,begin", false}, {"child-failure", "ok", "default,begin,authorize,child", true}} {
		t.Run(tc.name, func(t *testing.T) {
			log := &scopedNativeLog{}
			parent := componentArtifact(t, componentSpec("ScopedParent", "GET", "/scope-parent", nil), reflect.TypeFor[scopedNativeInput](), reflect.TypeFor[scopedNativeOutput]())
			parent.Handler = custom.New[scopedNativeInput, scopedNativeOutput](scopedNativeContract{})

			child := componentArtifact(t, componentSpec("ScopedChild", "GET", "/scope-child", nil), reflect.TypeFor[scopedNativeChildInput](), reflect.TypeFor[scopedNativeChildOutput]())
			child.Handler = custom.NewFunc(func(ctx context.Context, _ *scopedNativeChildInput) (*scopedNativeChildOutput, error) {
				if xlogger.FromContext(ctx) != log {
					return nil, errors.New("child authority lost")
				}
				log.Info("child")
				if tc.childFail {
					return nil, sentinel
				}
				return &scopedNativeChildOutput{Value: "native dependency"}, nil
			})

			parentRegistration, err := parent.Registration(registry.RegisteredComponent{Capabilities: xhandler.Capabilities{Logger: log}})
			if err != nil {
				t.Fatal(err)
			}
			childRegistration, err := child.Registration(registry.RegisteredComponent{Capabilities: xhandler.Capabilities{Logger: log}})
			if err != nil {
				t.Fatal(err)
			}
			rt, err := NewRuntime([]*registry.RegisteredComponent{parentRegistration, childRegistration})
			if err != nil {
				t.Fatal(err)
			}
			target := dexec.ComponentTarget{Component: parent.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/scope-parent"}}
			providers := []locator.Provider{handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) {
				log.Info("default")
				if tc.credential == "" {
					return nil, false, nil
				}
				return tc.credential, true, nil
			})}
			result, err := rt.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: target, Providers: providers})
			if tc.name == "success" {
				if err != nil || result.(*scopedNativeOutput).Value != "native dependency" {
					t.Fatal(result, err)
				}
			} else if err == nil {
				t.Fatal("failure missing")
			}
			if tc.childFail && !errors.Is(err, sentinel) {
				t.Fatal("child cause lost", err)
			}
			if strings.Join(log.events, ",") != tc.want {
				t.Fatal(log.events, err)
			}
		})
	}
}
func TestScopedDispatcherRejectsSeedReplayBeforeProviders(t *testing.T) {
	parent := componentArtifact(t, componentSpec("ScopedAdmission", "GET", "/scope-admission", nil), reflect.TypeFor[scopedNativeInput](), reflect.TypeFor[scopedNativeOutput]())
	parent.Handler = custom.New[scopedNativeInput, scopedNativeOutput](scopedNativeContract{})
	registered, err := parent.Registration(registry.RegisteredComponent{})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewRuntime([]*registry.RegisteredComponent{registered})
	if err != nil {
		t.Fatal(err)
	}
	target := dexec.ComponentTarget{Component: parent.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/scope-admission"}}
	for name, change := range map[string]func(*dexec.ComponentRequest){"input": func(r *dexec.ComponentRequest) { r.Input = &scopedNativeInput{} },
		"seed":           func(r *dexec.ComponentRequest) { r.ExtraInput = &dexec.InputSeed{} },
		"replay":         func(r *dexec.ComponentRequest) { r.Replay = &bindly.ReplayBinding{Only: true} },
		"warmup":         func(r *dexec.ComponentRequest) { r.Warmup = &dexec.ReaderWarmupRequest{} },
		"reader-options": func(r *dexec.ComponentRequest) { r.ReaderOptions = &dexec.ReaderOptions{} },
		"completion":     func(r *dexec.ComponentRequest) { r.Completion = func(xhandler.Outcome) {} },
		"omit":           func(r *dexec.ComponentRequest) { r.WarmupOmitInput = []string{"Credential"} }, "dry": func(r *dexec.ComponentRequest) { r.DryRun = true }, "prepare": func(r *dexec.ComponentRequest) { r.PrepareQuery = true }} {
		t.Run(name, func(t *testing.T) {
			r := dexec.ComponentRequest{Target: target, Providers: []locator.Provider{handlerprovider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) {
				t.Fatal("provider ran")
				return nil, false, nil
			})}}
			change(&r)
			if _, err := rt.InvokeComponent(t.Context(), r); err == nil {
				t.Fatal("admitted")
			}
		})
	}
}
