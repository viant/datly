package http

import (
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/gateway/openapi/openapi3"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

type routingMetadataInput struct {
	Tenant int
	ID     string
}

func TestRoutingEncodedMetadataTargetsSQLite(t *testing.T) {
	for _, mode := range []string{"", "escaped", "decoded"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			fixtures := []*configFixture{newConfigFixture(t), newConfigFixture(t)}
			paths := []string{"/api/items/{id}", "/api/items/a/{id}"}
			var components []*registry.RegisteredComponent
			for i, f := range fixtures {
				f.component.Key.Name = fmt.Sprintf("Metadata%d", i)
				f.component.Routes[0].Path = paths[i]
				f.component.Parameters = append(f.component.Parameters, &spec.Parameter{Name: "ID", TypeExpr: "string", Source: spec.BindSource{Kind: "path", Name: "id"}})
				f.component.Settings.Cache.Warmup.Cases[0].Set = append(f.component.Settings.Cache.Warmup.Cases[0].Set, &spec.CacheWarmupParam{Name: "ID", Values: []string{"b"}, ExcludeDefault: true})
				f.component.Routes[0].APIKeyHeader = "X-Key"
				f.component.Routes[0].APIKeyValue = fmt.Sprint(i)
				origin := []string{fmt.Sprintf("https://target%d.example", i)}
				f.component.Routes[0].CORS = &spec.CORS{AllowOrigins: &origin}
				require.NoError(t, f.db.ExecStatements(ctx, fmt.Sprintf("UPDATE records SET id=id+%d", i*100)))
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: f.component, InputType: reflect.TypeFor[routingMetadataInput](), OutputType: reflect.TypeFor[configOutput](), DirectViewField: "Rows"})
				require.NoError(t, err)
				reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
				require.NoError(t, err)
				components = append(components, &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[configOutput](), Reader: reader})
			}
			rt, err := druntime.NewRuntime(components)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
			selected := 0
			if mode == "decoded" {
				selected = 1
			}
			authorizedCache, authorizedWarm := "", ""
			config := Config{PathSemantics: mode, APIPrefix: "/api", Meta: Meta{CacheInvalidateURI: "/admin/cache", CacheWarmURI: "/admin/warm", OpenApiURI: "/admin/openapi", DocURI: " "}, OpenAPI: &OpenAPIConfig{Info: openapi3.Info{Title: "Routing metadata", Version: "1"}},
				CacheInvalidation: &CacheInvalidationConfig{Timeout: time.Second, Authorize: func(_ context.Context, _ *stdhttp.Request, target dexec.ComponentTarget) error {
					authorizedCache = target.Route.Path
					return nil
				}},
				Warmup: &WarmupConfig{Lifetime: NewWarmupLifetime(ctx), Timeout: time.Second, Completed: func(WarmupResult, error) {}, Authorize: func(_ context.Context, _ *stdhttp.Request, target dexec.ComponentTarget) error {
					authorizedWarm = target.Route.Path
					return nil
				}},
			}
			h, err := config.Build(ctx, HandlerInput{Runtime: rt, Components: components})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, h.Shutdown(ctx)) })
			send := func(method, path, key, origin string) *httptest.ResponseRecorder {
				r := httptest.NewRequest(method, path, nil)
				r.Header.Set("X-Key", key)
				if origin != "" {
					r.Header.Set("Origin", origin)
					r.Header.Set("Access-Control-Request-Method", "POST")
				}
				before, uri := *r.URL, r.RequestURI
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				require.Equal(t, before, *r.URL)
				require.Equal(t, uri, r.RequestURI)
				return w
			}
			read := func(i int) string {
				path := []string{"/api/items/b", "/api/items/a/b"}[i] + "?tenant=1"
				w := send("GET", path, fmt.Sprint(i), "")
				require.Equal(t, 200, w.Code, w.Body.String())
				return w.Body.String()
			}
			before := []string{read(0), read(1)}
			for i, f := range fixtures {
				require.NoError(t, f.db.ExecStatements(ctx, fmt.Sprintf("UPDATE records SET id=id+%d", 1000+i*1000)))
			}
			for _, kind := range []string{"cache", "warm"} {
				path := "/admin/" + kind + "/items/a%2Fb"
				preflight := send("OPTIONS", path, "", fmt.Sprintf("https://target%d.example", selected))
				require.Equal(t, 204, preflight.Code)
				require.Equal(t, fmt.Sprintf("https://target%d.example", selected), preflight.Header().Get("Access-Control-Allow-Origin"))
				denied := send("POST", path, fmt.Sprint(1-selected), "")
				require.Equal(t, 403, denied.Code)
			}
			invalidated := send("POST", "/admin/cache/items/a%2Fb", fmt.Sprint(selected), "")
			require.Equal(t, 200, invalidated.Code, invalidated.Body.String())
			require.Equal(t, paths[selected], authorizedCache)
			require.NotEqual(t, before[selected], read(selected))
			require.Equal(t, before[1-selected], read(1-selected))
			// Clear the selected cache, then prove warmup selected the same reader and its declared inputs.
			require.Equal(t, 200, send("POST", "/admin/cache/items/a%2Fb", fmt.Sprint(selected), "").Code)
			warmed := send("POST", "/admin/warm/items/a%2Fb?tenant=999&target=wrong", fmt.Sprint(selected), "")
			require.Equal(t, 200, warmed.Code, warmed.Body.String())
			require.Equal(t, paths[selected], authorizedWarm)
			var result WarmupResult
			require.NoError(t, json.Unmarshal(warmed.Body.Bytes(), &result))
			require.Equal(t, "GET:"+paths[selected], result.Target)
			require.Equal(t, 2, result.Groups)
			require.NoError(t, fixtures[selected].db.ExecStatements(ctx, "DROP TABLE records"))
			require.NotEqual(t, before[selected], read(selected))
			require.Equal(t, before[1-selected], read(1-selected))
			docs := send("GET", "/admin/openapi/items/a%2Fb?format=json", "", "")
			require.Equal(t, 200, docs.Code, docs.Body.String())
			var document struct {
				Paths map[string]any `yaml:"paths"`
			}
			require.NoError(t, yaml.Unmarshal(docs.Body.Bytes(), &document))
			require.Contains(t, document.Paths, paths[selected])
			require.NotContains(t, document.Paths, paths[1-selected])
			// Encoded metadata-prefix separators are recognized only by decoded host policy.
			prefix := send("GET", "/admin%2Fopenapi/items/a/b?format=json", "", "")
			want := 404
			if mode == "decoded" {
				want = 200
			}
			require.Equal(t, want, prefix.Code, prefix.Body.String())
			require.False(t, strings.Contains(authorizedCache, "%2F"))
		})
	}
}
