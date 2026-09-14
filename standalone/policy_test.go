package standalone

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http/httptest"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/authrecords"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
)

func TestConfiguredJWTThroughAuthoredPackage(t *testing.T) {
	f := fixture.New(t)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteConfig(t, func(c map[string]any) {
		c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/authrecords"}}
		c["JWTValidator"] = &verifier.Config{RSA: []*scy.Resource{{URL: "inline-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}
	})
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Registry: authrecords.Exports()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	if err = server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	token := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "reader", "exp": time.Now().Add(time.Hour).Unix()})
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, token string
		status      int
	}{{"missing", "", 401}, {"invalid", "Bearer invalid", 401}, {"valid", "Bearer " + signed, 200}} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/protected/1", nil)
			req.Header.Set("Authorization", tc.token)
			res := httptest.NewRecorder()
			server.manager.ServeHTTP(res, req)
			if res.Code != tc.status {
				t.Fatalf("%d %s", res.Code, res.Body.String())
			}
		})
	}
}
