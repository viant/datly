package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xcodec "github.com/viant/xdatly/codec"
)

func TestJwtClaimOriginalVerifierConfiguration(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := jwk.New(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := public.Set(jwk.KeyIDKey, "test-key"); err != nil {
		t.Fatal(err)
	}
	set, err := json.Marshal(map[string]any{"keys": []jwk.Key{public}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(set)
	}))
	defer server.Close()
	encoded, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	keyData := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})
	sign := func(method jwtv5.SigningMethod, key any, expiry time.Time) string {
		t.Helper()
		token := jwtv5.NewWithClaims(method, jwtv5.MapClaims{"sub": "user-seven", "user_id": 7, "exp": expiry.Unix()})
		token.Header["kid"] = "test-key"
		value, err := token.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	valid := sign(jwtv5.SigningMethodRS256, key, time.Now().Add(time.Hour))
	for _, mode := range []string{"public key", "certificate URL"} {
		t.Run(mode, func(t *testing.T) {
			config := &verifier.Config{RSA: []*scy.Resource{{URL: "test-public-key", Data: keyData}}}
			if mode == "certificate URL" {
				config = &verifier.Config{CertURL: server.URL}
			}
			service, err := New(context.Background(), &Config{JWTValidator: config})
			if err != nil {
				t.Fatal(err)
			}
			config.CertURL = "http://127.0.0.1:1/changed-after-init"
			codec, err := service.New(&xcodec.Config{Body: JwtClaim, SourceType: reflect.TypeFor[string](), DestinationType: reflect.TypeFor[*jwt.Claims]()})
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name, raw string
				valid     bool
			}{
				{"raw token", valid, true}, {"bearer", "Bearer " + valid, true},
				{"missing", "", false}, {"malformed", "Bearer malformed", false},
				{"wrong signature", sign(jwtv5.SigningMethodRS256, other, time.Now().Add(time.Hour)), false},
				{"expired", sign(jwtv5.SigningMethodRS256, key, time.Now().Add(-time.Hour)), false},
				{"wrong algorithm", sign(jwtv5.SigningMethodHS256, []byte{}, time.Now().Add(time.Hour)), false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					value, err := codec.Value(context.Background(), tc.raw)
					if tc.valid {
						if err != nil {
							t.Fatal(err)
						}
						claims, ok := value.(*jwt.Claims)
						if !ok || claims.UserID != 7 || claims.Subject != "user-seven" {
							t.Fatalf("claims=%+v", value)
						}
					} else if err == nil || value != nil {
						t.Fatalf("invalid credential accepted: value=%T error=%v", value, err)
					}
				})
			}
		})
	}
}
