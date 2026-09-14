package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	runtimeauth "github.com/viant/datly/runtime/auth"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/scy/auth/jwt"
	xhandler "github.com/viant/xdatly/handler"
)

type injectorJWTInput struct {
	JWT     *jwt.Claims   `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim"`
	Subject string        `parameter:"Subject,kind=param,in=JWT.RegisteredClaims.Subject"`
	Request *http.Request `parameter:"Request,kind=http_request"`
}

func TestInjectorFinalizerCurrentDeclaredJWTSQLite(t *testing.T) {
	key := testharness.NewJWT(t)
	factory, err := runtimeauth.New(context.Background(), &runtimeauth.Config{JWTValidator: key.Config()})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, token      string
		declared, denied bool
	}{
		{"current", key.Sign(t, "current-user", time.Now().Add(time.Hour)), true, false},
		{"changed", key.Sign(t, "next-user", time.Now().Add(time.Hour)), true, false},
		{"expired", key.Sign(t, "expired-user", time.Now().Add(-time.Hour)), true, true},
		{"invalid", "Bearer invalid", true, true},
		{"missing", "", true, true},
		{"undeclared", "Bearer invalid", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := testharness.NewSQLiteHarness(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE audit(subject TEXT)"); err != nil {
				t.Fatal(err)
			}
			parent := componentSpec("JWTParent", "POST", "/jwt-parent", nil)
			pa := componentArtifact(t, parent, reflect.TypeFor[injectorParentInput](), reflect.TypeFor[injectorParentOutput]())
			child := componentSpec("JWTChild", "POST", "/jwt-child", nil)
			inputType := reflect.TypeFor[struct{}]()
			if test.declared {
				inputType = reflect.TypeFor[injectorJWTInput]()
			}
			ca, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: child, InputType: inputType, OutputType: reflect.TypeFor[injectorChildOutput](), CodecFactory: factory})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			req := httptest.NewRequest("POST", "/jwt-parent", nil)
			req.Header.Set("Authorization", "Bearer stale")
			scope, err := request.New(req)
			if err != nil {
				t.Fatal(err)
			}
			scope = scope.WithHeader("Authorization", test.token)
			rt, err := NewRuntime([]*registry.RegisteredComponent{
				{Component: pa.Component, Input: pa.Input, OutputType: reflect.TypeFor[injectorParentOutput](), Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
					o := &injectorParentOutput{Enabled: true}
					o.hook = func(ctx context.Context, l xhandler.InjectorLookup) error {
						b, err := l(context.Background(), xhandler.Route{Method: "POST", URL: "/jwt-child"})
						if err != nil {
							return err
						}
						return b.Bind(context.Background(), o)
					}
					return o, nil
				})},
				{Component: ca.Component, Input: ca.Input, OutputType: reflect.TypeFor[injectorChildOutput](), DataSource: dml.Source{DB: h.DB}, Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
					calls++
					subject := "undeclared"
					if test.declared {
						input := inv.Input.(*injectorJWTInput)
						if input.JWT == nil || input.JWT.Subject != input.Subject || input.Request != req {
							return nil, fmt.Errorf("canonical JWT/request binding: %+v", input)
						}
						subject = input.Subject
					}
					dep := struct {
						DML xhandler.DML `bind:"kind=dml,required"`
					}{}
					if err := inv.Binder.Bind(ctx, &dep); err != nil {
						return nil, err
					}
					if err := dep.DML.Execute("INSERT INTO audit VALUES(?)", subject); err != nil {
						return nil, err
					}
					return &injectorChildOutput{Name: subject}, nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := rt.ExecuteRoute(context.Background(), "POST", "/jwt-parent", scope)
			if test.denied {
				if err == nil || calls != 0 {
					t.Fatalf("authorization err=%v calls=%d", err, calls)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := "undeclared"
			if test.declared {
				want = test.name + "-user"
				if test.name == "changed" {
					want = "next-user"
				}
			}
			if result.(*injectorParentOutput).Name != want || calls != 1 {
				t.Fatalf("result=%+v calls=%d", result, calls)
			}
			var subject string
			if err := h.DB.QueryRow("SELECT subject FROM audit").Scan(&subject); err != nil || subject != want {
				t.Fatalf("stored=%q err=%v", subject, err)
			}
		})
	}
}
