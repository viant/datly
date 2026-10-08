package provider

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
	xauth "github.com/viant/xdatly/auth"
)

func TestVerifierOnlyProviderPublicKeyAndRemoteCertificates(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	cert := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "alg": "RS256", "use": "sig", "kid": "fixture", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": "AQAB"}}})
	}))
	defer cert.Close()
	for name, cfg := range map[string]*verifier.Config{"public key": {RSA: []*scy.Resource{{URL: "synthetic-public", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}, "remote cert": {CertURL: cert.URL}} {
		t.Run(name, func(t *testing.T) {
			service, err := NewVerifier(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if service.Verifier() == nil || service.Signer() != nil {
				t.Fatal("verifier-only acquired signer or lacks verifier")
			}
			if _, err = service.Authenticator(xauth.VendorDefault); !errors.Is(err, xauth.ErrUnsupportedVendor) {
				t.Fatal("default authenticator invented", err)
			}
			authn, err := service.Authenticator(xauth.VendorJWT)
			if err != nil {
				t.Fatal(err)
			}
			mint := func(exp time.Time) string {
				token := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "synthetic", "exp": exp.Unix()})
				token.Header["kid"] = "fixture"
				signed, e := token.SignedString(key)
				if e != nil {
					t.Fatal(e)
				}
				return signed
			}
			claims, err := authn.VerifyIdentity(context.Background(), mint(time.Now().Add(time.Minute)))
			if err != nil || claims.Subject != "synthetic" {
				t.Fatal("signed token denied", err)
			}
			if _, err = authn.VerifyIdentity(context.Background(), mint(time.Now().Add(-time.Minute))); err == nil {
				t.Fatal("expired token accepted")
			}
			if _, err = authn.VerifyIdentity(context.Background(), "not-signed"); err == nil {
				t.Fatal("invalid token accepted")
			}
			if _, err = authn.BasicAuth(context.Background(), "caller", "password"); !errors.Is(err, xauth.ErrUnsupportedVendor) {
				t.Fatal("password authentication enabled")
			}
		})
	}
}
