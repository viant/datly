package runtime

import (
	"context"
	"errors"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/scy/auth/jwt"
	xhandler "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

type scopedJWTInput struct {
	Jwt   *jwt.Claims                `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	Child *scopedNativeChildOutput   `parameter:",kind=component,in=GET:/scope-jwt-child,scope=lazy"`
	Has   *struct{ Jwt, Child bool } `setMarker:"true"`
}
type scopedJWTContract struct{}

func (scopedJWTContract) BindsInputByScope() bool { return true }
func (scopedJWTContract) Exec(ctx context.Context, s xhandler.Session, i *scopedJWTInput, o *scopedNativeOutput) error {
	log := xlogger.FromContext(ctx)
	log.Info("begin")
	if i.Jwt == nil || i.Child != nil || i.Has == nil || !i.Has.Jwt || i.Has.Child {
		return errors.New("JWT default prebinding missing or child eager")
	}
	b := s.Binder().(xhandler.ScopedBinder)
	if i.Jwt == nil || i.Has == nil || !i.Has.Jwt || i.Has.Child {
		return errors.New("JWT state")
	}
	if i.Jwt.Subject == "denied" {
		return errors.New("source authorization denied")
	}
	log.Info("authorize")
	marker := i.Has
	originalJWT := i.Jwt
	if err := b.BindScope(ctx, i, "lazy"); err != nil {
		return err
	}
	if marker != i.Has || originalJWT != i.Jwt || !i.Has.Child || i.Child == nil {
		return errors.New("child state")
	}
	o.Value = i.Child.Value
	log.Info("end")
	return nil
}
func TestScopedNativeJWTDefaultScopeBeforeActualComposedChild(t *testing.T) {
	key := testharness.NewJWT(t)
	factory, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, token string
		success     bool
	}{{"missing", "", false}, {"malformed", "Bearer invalid", false}, {"expired", key.Sign(t, "approved", time.Now().Add(-time.Hour)), false}, {"denied", key.Sign(t, "denied", time.Now().Add(time.Hour)), false}, {"valid", key.Sign(t, "approved", time.Now().Add(time.Hour)), true}} {
		t.Run(tc.name, func(t *testing.T) {
			log := &scopedNativeLog{}
			parent, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: componentSpec("ScopedJWT", "GET", "/scope-jwt", nil), InputType: reflect.TypeFor[scopedJWTInput](), OutputType: reflect.TypeFor[scopedNativeOutput](), Handler: custom.New[scopedJWTInput, scopedNativeOutput](scopedJWTContract{}), CodecFactory: factory})
			if err != nil {
				t.Fatal(err)
			}
			child := componentArtifact(t, componentSpec("ScopedJWTChild", "GET", "/scope-jwt-child", nil), reflect.TypeFor[scopedNativeChildInput](), reflect.TypeFor[scopedNativeChildOutput]())
			calls := 0
			child.Handler = custom.NewFunc(func(ctx context.Context, _ *scopedNativeChildInput) (*scopedNativeChildOutput, error) {
				calls++
				log.Info("child")
				return &scopedNativeChildOutput{Value: "verified child"}, nil
			})
			pr, err := parent.Registration(registry.RegisteredComponent{Capabilities: xhandler.Capabilities{Logger: log}})
			if err != nil {
				t.Fatal(err)
			}
			cr, err := child.Registration(registry.RegisteredComponent{Capabilities: xhandler.Capabilities{Logger: log}})
			if err != nil {
				t.Fatal(err)
			}
			rt, err := NewRuntime([]*registry.RegisteredComponent{pr, cr})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("GET", "/scope-jwt", nil)
			if tc.token != "" {
				req.Header.Set("Authorization", tc.token)
			}
			scope, err := request.New(req)
			if err != nil {
				t.Fatal(err)
			}
			headerCalls := 0
			providers := []locator.Provider{handlerprovider.Named("header", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
				headerCalls++
				log.Info("default")
				value := req.Header.Get(name)
				return value, value != "", nil
			})}
			result, err := rt.invokeComponent(t.Context(), dexec.ComponentRequest{Providers: providers, Target: dexec.ComponentTarget{Component: parent.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/scope-jwt"}}}, scope)
			if tc.success {
				if err != nil || result.(*scopedNativeOutput).Value != "verified child" || calls != 1 || strings.Join(log.events, ",") != "default,begin,authorize,child,end" {
					t.Fatal(err, calls, log.events)
				}
			} else {
				want := "default"
				if tc.name == "denied" {
					want = "default,begin"
				}
				if err == nil || calls != 0 || strings.Join(log.events, ",") != want {
					t.Fatal(err, calls, log.events)
				}
			}
			if headerCalls != 1 {
				t.Fatalf("header resolved %d times", headerCalls)
			}
			if tc.name == "missing" {
				var owner *bindly.BindingError
				if !errors.As(err, &owner) || owner.Name != "Jwt" || owner.Code != 401 {
					t.Fatalf("actual missing owner %#v error%v", owner, err)
				}
			}
		})
	}
}
