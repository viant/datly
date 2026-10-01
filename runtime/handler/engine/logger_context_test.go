package engine

import (
	"context"
	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	provider "github.com/viant/datly/runtime/handler/provider"
	xlogger "github.com/viant/xdatly/logger"
	"reflect"
	"testing"
)

type contextLogger struct{ name string }

func (*contextLogger) Debug(string, ...any) {}
func (*contextLogger) Info(string, ...any)  {}
func (*contextLogger) Warn(string, ...any)  {}
func (*contextLogger) Error(string, ...any) {}

func TestEngineInvocationLoggerMatchesStaticAuthority(t *testing.T) {
	parent, child := &contextLogger{name: "parent"}, &contextLogger{name: "child"}
	for _, tc := range []struct {
		name                  string
		root, component, want xlogger.Logger
	}{
		{"absent", nil, nil, nil}, {"root", parent, nil, parent}, {"component override", parent, child, child},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			if tc.root != nil {
				root, err = root.ForScope(provider.Static(rhandler.LoggerCapabilityKey, tc.root))
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err = New().Execute(xlogger.WithContext(context.Background(), child), Request{
				Injector: root, Input: testRouteInput(t, reflect.TypeFor[engineInput]()), Capabilities: rhandler.InvocationCapabilities{Logger: tc.component},
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					actual := xlogger.FromContext(ctx)
					if actual != tc.want {
						t.Fatal("invocation context logger differs from configured authority")
					}
					bound, found, err := invocation.Binder.Lookup(ctx, rhandler.LoggerCapabilityKey)
					if err != nil {
						t.Fatal(err)
					}
					if tc.want != nil && (!found || bound != actual) {
						t.Fatal("static binder and context identity differ")
					}
					return nil, nil
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
