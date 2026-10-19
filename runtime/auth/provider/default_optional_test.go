package provider

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	xauth "github.com/viant/xdatly/auth"
	"testing"
	"time"
)

func TestSignerVerifierWithoutDefaultAuthenticator(t *testing.T) {
	ctx := context.Background()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{JWTSigner: &signer.Config{RSA: &scy.Resource{URL: "synthetic-private", Data: pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})}}, JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "synthetic-public", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}}
	service, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if service.Signer() == nil || service.Verifier() == nil {
		t.Fatal("configured crypto capability lost")
	}
	for _, v := range []xauth.Vendor{"", xauth.VendorDefault} {
		if _, err := service.Authenticator(v); !errors.Is(err, xauth.ErrUnsupportedVendor) {
			t.Fatal("default invented", err)
		}
	}
	authn, err := service.Authenticator(xauth.VendorJWT)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := service.Signer().Create(time.Minute, map[string]any{"sub": "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authn.VerifyIdentity(ctx, signed); err != nil {
		t.Fatal("configured signer token denied", err)
	}
	wrong, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		key  *rsa.PrivateKey
		exp  time.Time
	}{{"valid", key, time.Now().Add(time.Minute)}, {"expired", key, time.Now().Add(-time.Minute)}, {"wrong signature", wrong, time.Now().Add(time.Minute)}} {
		t.Run(tc.name, func(t *testing.T) {
			token, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "synthetic", "exp": tc.exp.Unix()}).SignedString(tc.key)
			if err != nil {
				t.Fatal(err)
			}
			_, err = authn.VerifyIdentity(ctx, token)
			if (err == nil) != (tc.name == "valid") {
				t.Fatal("unexpected verification", err)
			}
		})
	}
	if _, err := authn.VerifyIdentity(ctx, "malformed"); err == nil {
		t.Fatal("malformed accepted")
	}
	if _, err := authn.BasicAuth(ctx, "user", "password"); !errors.Is(err, xauth.ErrUnsupportedVendor) {
		t.Fatal(err)
	}
	if _, err := authn.ReissueIdentityToken(ctx, "refresh", "subject"); !errors.Is(err, xauth.ErrUnsupportedVendor) {
		t.Fatal(err)
	}
	if err := authn.ResetCredentials(ctx, "user", "password"); !errors.Is(err, xauth.ErrUnsupportedVendor) {
		t.Fatal(err)
	}
	configured := cfg
	configured.Default = defaultAuthenticator{}
	withDefault, err := New(ctx, configured)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []xauth.Vendor{"", xauth.VendorDefault} {
		if _, err := withDefault.Authenticator(v); err != nil {
			t.Fatal(err)
		}
	}
	for i, broken := range []Config{{JWTValidator: cfg.JWTValidator}, {JWTSigner: cfg.JWTSigner}, {JWTSigner: &signer.Config{RSA: cfg.JWTSigner.RSA, HMAC: &scy.Resource{URL: "synthetic-conflict", Data: []byte("key")}}, JWTValidator: cfg.JWTValidator}, {JWTSigner: cfg.JWTSigner, JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "invalid-synthetic-public", Data: []byte("invalid key")}}}}} {
		if _, err := New(ctx, broken); err == nil {
			t.Fatalf("incomplete or invalid crypto accepted case %d", i)
		}
	}
}
