package http

import (
	"context"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	dexec "github.com/viant/datly/exec"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
)

// TestHTTPAuthorizeBindsTrustedScopeProviderSQLite proves that a provider bound
// by the Authorize hook reaches the compiled component's input through the
// ordinary handler path, that transport input of the same name cannot override
// it, and that a bound-kind parameter is unreachable when the hook binds nothing.
func TestHTTPAuthorizeBindsTrustedScopeProviderSQLite(t *testing.T) {
	newHandler := func(t *testing.T, authorize func(context.Context, *stdhttp.Request, dexec.ComponentTarget) error) *Handler {
		t.Helper()
		f := newConfigFixture(t)
		f.component.Settings = nil
		// The Tenant parameter is populated only by a trusted "scope" provider.
		f.component.Parameters[0].Source = spec.BindSource{Kind: "scope", Name: "tenant"}
		h, err := (Config{Authorize: authorize}).NewHandler(f.runtime(t), nil, "test")
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	bindTenant := func(tenant int) func(context.Context, *stdhttp.Request, dexec.ComponentTarget) error {
		return func(ctx context.Context, _ *stdhttp.Request, target dexec.ComponentTarget) error {
			if target.Route.Path != "/api/records" {
				return errors.New("unexpected target")
			}
			return dexec.BindScope(ctx, handlerprovider.Named("scope", func(_ context.Context, targetType reflect.Type, name string) (any, bool, error) {
				if name != "tenant" {
					return nil, false, nil
				}
				if targetType != nil && targetType.Kind() != reflect.Int {
					return nil, false, errors.New("scope value type mismatch")
				}
				return tenant, true, nil
			}))
		}
	}
	get := func(t *testing.T, h *Handler, uri string) (int, string) {
		t.Helper()
		req := httptest.NewRequest("GET", uri, nil)
		req.Header.Set("tenant", "2")
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res.Code, res.Body.String()
	}

	t.Run("bound scope selects rows", func(t *testing.T) {
		code, body := get(t, newHandler(t, bindTenant(1)), "/api/records")
		if code != 200 || !strings.Contains(body, `"id":11`) || !strings.Contains(body, `"id":12`) || strings.Contains(body, `"id":22`) {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("distinct principals receive distinct scope", func(t *testing.T) {
		code, body := get(t, newHandler(t, bindTenant(2)), "/api/records")
		if code != 200 || strings.Contains(body, `"id":11`) || !strings.Contains(body, `"id":22`) {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("query and header cannot override bound scope", func(t *testing.T) {
		code, body := get(t, newHandler(t, bindTenant(1)), "/api/records?tenant=2&Tenant=2&scope=2")
		if code != 200 || strings.Contains(body, `"id":22`) || !strings.Contains(body, `"id":11`) {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("hook that binds nothing leaves the required scope unresolved", func(t *testing.T) {
		code, body := get(t, newHandler(t, func(context.Context, *stdhttp.Request, dexec.ComponentTarget) error { return nil }), "/api/records?tenant=1")
		if code == 200 || strings.Contains(body, `"id":11`) || strings.Contains(body, `"id":22`) {
			t.Fatalf("unbound scope parameter released data: %d %s", code, body)
		}
	})
	t.Run("hook denial is honored before binding", func(t *testing.T) {
		code, body := get(t, newHandler(t, func(context.Context, *stdhttp.Request, dexec.ComponentTarget) error { return errors.New("denied") }), "/api/records")
		if code != 403 || strings.Contains(body, `"id":`) {
			t.Fatalf("%d %s", code, body)
		}
	})
	t.Run("binding outside an authorize hook is unavailable", func(t *testing.T) {
		if err := dexec.BindScope(context.Background(), handlerprovider.Named("scope", nil)); !errors.Is(err, dexec.ErrScopeBindingUnavailable) {
			t.Fatalf("expected unavailable binding, got %v", err)
		}
	})
}
