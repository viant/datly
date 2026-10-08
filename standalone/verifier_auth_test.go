package standalone

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/standalone/testdata/verifierauth"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
)

func TestVerifierOnlyLinkedHandlerBindsAuthWithoutSignerOrSource(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{BaseDir: filepath.Join(t.TempDir(), "no-source"), Endpoint: config.Endpoint{Address: "127.0.0.1:0"}, GoBootstrap: &config.Packages{Packages: []string{"github.com/viant/datly/standalone/testdata/verifierauth"}, LinkedOnly: true}, JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "synthetic-public", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}}
	s, err := New(context.Background(), Options{Config: cfg, Holders: []any{verifierauth.Datly}, RequireLinked: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	t.Setenv("PATH", t.TempDir())
	if err = s.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	for _, valid := range []bool{true, false} {
		req := httptest.NewRequest("GET", "/verify-only", nil)
		if valid {
			token, e := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "synthetic-reader", "exp": time.Now().Add(time.Minute).Unix()}).SignedString(key)
			if e != nil {
				t.Fatal(e)
			}
			req.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		if !valid {
			if w.Code != 401 {
				t.Fatalf("missing credential status%d %s", w.Code, w.Body)
			}
			continue
		}
		if w.Code != 200 {
			t.Fatalf("verifier-only binding%d %s", w.Code, w.Body)
		}
		var output verifierauth.Output
		if json.Unmarshal(w.Body.Bytes(), &output) != nil || output.Subject != "synthetic-reader" || !output.Verifier || output.Signer || output.DefaultEnabled {
			t.Fatalf("narrow Auth output %s", w.Body)
		}
	}
}
