package bootstrap_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/scy/auth/jwt"
)

func TestContractStateBindingUsesNativeJWTCodec(t *testing.T) {
	key := testharness.NewJWT(t)
	factory, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: key.Config()})
	if err != nil {
		t.Fatal(err)
	}
	plans := bootstrap.ContractStatePlans(factory, nil, nil)
	for _, tc := range []struct {
		name, token string
		valid       bool
	}{{"missing", "", false}, {"malformed", "Bearer invalid", false}, {"expired", key.Sign(t, "caller", time.Now().Add(-time.Hour)), false}, {"valid", key.Sign(t, "caller", time.Now().Add(time.Hour)), true}} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set("Authorization", tc.token)
			req.Header.Set("X-Debug", "true")
			source, err := request.New(req)
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			root, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			scope, err := root.ForScope(source.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				Claims *jwt.Claims `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
				Debug  bool        `parameter:",kind=header,in=X-Debug"`
			}
			err = plans.Binder(scope).Bind(t.Context(), &state)
			if tc.valid {
				if err != nil || state.Claims == nil || state.Claims.Subject != "caller" || !state.Debug {
					t.Fatalf("state=%+v err=%v", state, err)
				}
			} else if err == nil {
				t.Fatal("unverified credential accepted")
			}
		})
	}
}

func TestContractStateRejectsUnregisteredCodec(t *testing.T) {
	root, _ := bindly.NewInjector()
	var state struct {
		Count int `parameter:",kind=header,in=X-Count" codec:"MissingCodec"`
	}
	if err := bootstrap.ContractStatePlans(nil, nil, nil).Binder(root).Bind(t.Context(), &state); err == nil {
		t.Fatal("codec was silently ignored")
	}
}
