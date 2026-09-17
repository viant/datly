package application_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/internal/testharness"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
)

type indexedWarmupInput struct {
	Tenant int
	Auth   *indexedWarmupAuthOutput `parameter:"Auth,kind=component,in=GET:/auth"`
}
type indexedWarmupAuthInput struct {
	Claims *indexedWarmupAuthOutput `parameter:"Claims,kind=component,in=GET:/claims"`
}
type indexedWarmupClaimsInput struct {
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim"`
}
type indexedWarmupCycleInput struct {
	Auth *indexedWarmupAuthOutput `parameter:"Auth,kind=component,in=GET:/auth"`
}
type indexedWarmupAuthOutput struct{ Subject string }

func TestIndexedWarmupPreloadsTransitiveCredentials(t *testing.T) {
	for _, mode := range []string{"transitive", "overlapping roots", "missing dependency", "cycle"} {
		t.Run(mode, func(t *testing.T) { testIndexedWarmupCredentials(t, mode) })
	}
}

func testIndexedWarmupCredentials(t *testing.T, mode string) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/warmup"}).Write(t, root)
	for _, name := range []string{"records", "auth", "claims", "unused"} {
		if mode == "missing dependency" && name == "claims" {
			continue
		}
		writeFixture(t, root, name+"/holder.go", `package `+name+`
import "github.com/viant/xdatly"
type Input struct{}
type Output struct{}
type Holder struct { Route xdatly.Component[Input, Output] `+"`"+`component:"`+name+`,path=/`+name+`,method=GET"`+"`"+` }
`)
	}
	snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, Include: []string{"example.com/warmup/..."}}}).Build(ctx)
	require.NoError(t, err)
	f := &httpReloadFixture{}
	f.init(t)
	base, err := f.compile(1, httpReloadConfig(), nil)(ctx, typecatalog.NewCatalog())
	require.NoError(t, err)
	settings := base.Components[0].Component.Settings
	snapshot, err = snapshot.Transform(func(c *spec.Component) {
		if c.Key.Name == "records" {
			c.Settings = settings.Clone()
		}
	})
	require.NoError(t, err)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	factory, err := runtimeauth.New(ctx, &runtimeauth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "fixture-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}})
	require.NoError(t, err)
	var mu sync.Mutex
	loads := map[string]int{}
	var calls atomic.Int32
	materializer := bootstrapindex.MaterializeFunc(func(ctx context.Context, entry *bootstrapindex.Entry, _ bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
		mu.Lock()
		loads[entry.Key().Name]++
		mu.Unlock()
		c := entry.Component
		var input, output reflect.Type
		switch c.Key.Name {
		case "records":
			input, output = reflect.TypeFor[indexedWarmupInput](), reflect.TypeFor[httpReloadOutput]()
			c.Parameters = base.Components[0].Component.Parameters
			c.RootView = base.Components[0].Component.RootView
		case "auth":
			input, output = reflect.TypeFor[indexedWarmupAuthInput](), reflect.TypeFor[indexedWarmupAuthOutput]()
		case "claims":
			input, output = reflect.TypeFor[indexedWarmupClaimsInput](), reflect.TypeFor[indexedWarmupAuthOutput]()
			if mode == "cycle" {
				input = reflect.TypeFor[indexedWarmupCycleInput]()
			}
		default:
			return nil, fmt.Errorf("unrelated component must stay lazy")
		}
		view := ""
		if c.Key.Name == "records" {
			view = "Rows"
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: input, OutputType: output, DirectViewField: view, CodecFactory: factory})
		if err != nil {
			return nil, err
		}
		reg := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: output}
		switch c.Key.Name {
		case "records":
			reg.Reader, err = artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
		case "auth":
			reg.Handler = custom.NewFunc[indexedWarmupAuthInput, indexedWarmupAuthOutput](func(_ context.Context, in *indexedWarmupAuthInput) (*indexedWarmupAuthOutput, error) {
				calls.Add(1)
				return in.Claims, nil
			})
		case "claims":
			reg.Handler = custom.NewFunc[indexedWarmupClaimsInput, indexedWarmupAuthOutput](func(_ context.Context, in *indexedWarmupClaimsInput) (*indexedWarmupAuthOutput, error) {
				calls.Add(1)
				return &indexedWarmupAuthOutput{Subject: in.JWT.Subject}, nil
			})
		}
		return &bootstrapindex.Loaded{Registration: reg}, err
	})
	manager, err := application.New(nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Shutdown(context.Background())) })
	entry, _, _, ok := snapshot.Route("GET", "/records")
	require.True(t, ok)
	roots := []spec.Key{entry.Key()}
	if mode == "overlapping roots" {
		auth, _, _, found := snapshot.Route("GET", "/auth")
		require.True(t, found)
		roots = append(roots, entry.Key(), auth.Key())
	}
	err = manager.Reload(ctx, application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{Index: snapshot, Materializer: materializer, Preload: roots, HTTP: httpReloadConfig()}, nil
	}})
	if mode == "missing dependency" {
		require.ErrorContains(t, err, "component dependency route contract not found: GET:/claims")
		return
	}
	if mode == "cycle" {
		require.ErrorContains(t, err, "component input dependency cycle")
		return
	}
	require.NoError(t, err)
	mu.Lock()
	actual := map[string]int{}
	for k, v := range loads {
		actual[k] = v
	}
	mu.Unlock()
	require.Equal(t, map[string]int{"records": 1, "auth": 1, "claims": 1}, actual)
	require.Zero(t, calls.Load(), "preloading must not execute authentication handlers")
	token, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "fixture", "exp": time.Now().Add(time.Hour).Unix()}).SignedString(key)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, token, admin string
		status             int
	}{
		{"missing", "", "admin", 401}, {"invalid", "Bearer invalid", "admin", 401},
		{"non-admin", "Bearer " + token, "", 403}, {"verified", "Bearer " + token, "admin", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/api/cache/warmup/records", nil)
			req.Header.Set("Authorization", tc.token)
			req.Header.Set("X-Admin", tc.admin)
			res := httptest.NewRecorder()
			manager.ServeHTTP(res, req)
			require.Equal(t, tc.status, res.Code, res.Body.String())
			if tc.status != 200 {
				require.Zero(t, calls.Load(), "rejected credentials must not execute auth handlers or warmup")
			}
		})
	}
	require.Positive(t, calls.Load(), "verified warmup must bind the transitive credential")
}
