package standalone

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/authrecords"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
)

func TestConfiguredWarmupJWTIsNotAdministrator(t *testing.T) {
	f := fixture.New(t)
	f.Services(t, "")
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	cfg.GoBootstrap.Packages = []string{fixture.Module + "/authrecords"}
	cfg.APIKeys = gateway.APIKeys{{URI: "/protected", Header: "X-Read", Value: "read-key"}}
	cfg.OpenAPI.StartupExports = nil
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	cfg.JWTValidator = &verifier.Config{RSA: []*scy.Resource{{URL: "inline-public-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}
	dql := fmt.Sprintf(`#setting($_ = $route('/protected/{id}', 'GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $cache(true, '1m').WithLocation('%s'))
#setting($_ = $cache_warmup('id', 'IndexParameter=id', 'id=1'))
SELECT records.* FROM (SELECT id,name FROM records WHERE id=:ID) records`, filepath.ToSlash(filepath.Join(f.Root, "protected-cache")))
	if err = os.WriteFile(filepath.Join(f.Root, "authrecords/Protected.dql"), []byte(dql), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := New(context.Background(), Options{Config: cfg, Registry: authrecords.Exports()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	if err = s.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	token := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "user", "exp": time.Now().Add(time.Hour).Unix()})
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, admin, token string
		status             int
	}{{"token is not admin", "", "Bearer " + signed, 403}, {"missing JWT", "admin-key", "", 401}, {"bad JWT", "admin-key", "Bearer invalid", 401}, {"admin key and declared JWT", "admin-key", "Bearer " + signed, 200}} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/warm/protected/1", nil)
			req.Header.Set("X-Read", "read-key")
			req.Header.Set("X-Admin", tc.admin)
			req.Header.Set("Authorization", tc.token)
			res := httptest.NewRecorder()
			s.manager.ServeHTTP(res, req)
			if res.Code != tc.status {
				t.Fatalf("%d %s", res.Code, res.Body.String())
			}
		})
	}
}
