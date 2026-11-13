package http

import (
	"context"
	"github.com/viant/authz"
	druntime "github.com/viant/datly/runtime"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func BenchmarkAuthorizationHTTP(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "protected"
		}
		b.Run(name, func(b *testing.B) {
			rt, err := druntime.NewRuntime(nil)
			if err != nil {
				b.Fatal(err)
			}
			defer rt.Shutdown(context.Background())
			c := Config{}
			if enabled {
				c.Authorization = &AuthorizationConfig{Provider: "fixture", DefaultPolicy: "read", Policies: map[string]authz.Policy{"read": {Mode: "protected", Rule: &authz.Rule{Kind: "subject", Value: "*"}}}}
				c.AuthorizationProviders = map[string]AuthorizationProvider{"fixture": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
					return ctx, authz.Facts{Subject: "user", Tenant: "tenant", Issuer: "issuer", ValidUntil: time.Now().Add(time.Hour)}, nil
				}}
			}
			h, err := c.Build(context.Background(), HandlerInput{Runtime: rt})
			if err != nil {
				b.Fatal(err)
			}
			req := httptest.NewRequest("GET", DefaultStatusURI, nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, req)
				if w.Code != 200 {
					b.Fatal(w.Code)
				}
			}
		})
	}
}
