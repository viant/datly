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
	"github.com/viant/datly/internal/logging"
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
					ctx := logging.WithIdentityObservation(context.Background())
					value, err := codec.Value(ctx, tc.raw)
					observed, verified, conflict := logging.IdentitySnapshot(ctx)
					if verified != tc.valid || conflict || (tc.valid && observed.UserID != 7) {
						t.Fatal("signature evidence mismatch")
					}
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

func TestJwtClaimPolicyRejectsWrongIssuerAudienceAndMissingSubject(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	config := &Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "policy-public-key",
		Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})}}},
		ClaimPolicy: &ClaimPolicy{Issuer: "https://identity.example", Audience: "studio-web", RequireSubject: true}}
	service, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := service.New(&xcodec.Config{Body: JwtClaim, SourceType: reflect.TypeFor[string](), DestinationType: reflect.TypeFor[*jwt.Claims]()})
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []struct {
		name, issuer, audience, subject string
		allowed                         bool
	}{
		{"matching identity", "https://identity.example", "studio-web", "alice", true},
		{"wrong issuer", "https://other.example", "studio-web", "alice", false},
		{"wrong audience", "https://identity.example", "other-client", "alice", false},
		{"missing subject", "https://identity.example", "studio-web", "", false},
	} {
		t.Run(candidate.name, func(t *testing.T) {
			payload := jwtv5.MapClaims{"iss": candidate.issuer, "aud": candidate.audience, "sub": candidate.subject,
				"exp": time.Now().Add(time.Hour).Unix(), "user_id": 7, "username": "alice", "email": "alice@example.test", "scope": "read", "dat": map[string]any{"secret": "excluded"}}
			value, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, payload).SignedString(key)
			if err != nil {
				t.Fatal(err)
			}
			ctx := logging.WithIdentityObservation(context.Background())
			claims, err := codec.Value(ctx, "Bearer "+value)
			observed, verified, conflict := logging.IdentitySnapshot(ctx)
			if verified != candidate.allowed || conflict {
				t.Fatal("claim policy evidence mismatch")
			}
			if candidate.allowed {
				expected := logging.Identity{UserID: 7, Username: "alice", Email: "alice@example.test", Scope: "read"}
				if observed != expected {
					t.Fatal("scalar identity mismatch")
				}
				claims.(*jwt.Claims).Username = "mutated"
				if got, _, _ := logging.IdentitySnapshot(ctx); got != expected {
					t.Fatal("mutable claims leaked")
				}
				if _, err := codec.Value(ctx, "malformed"); err == nil {
					t.Fatal("malformed token accepted")
				}
				if got, verified, conflict := logging.IdentitySnapshot(ctx); got != expected || !verified || conflict {
					t.Fatal("failed verification replaced successful evidence")
				}
				payload["user_id"] = 8
				different, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, payload).SignedString(key)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := codec.Value(ctx, different); err != nil {
					t.Fatal(err)
				}
				if got, verified, conflict := logging.IdentitySnapshot(ctx); got != (logging.Identity{}) || verified || !conflict {
					t.Fatal("verified nested conflict retained attribution")
				}
				if _, err := codec.Value(ctx, value); err != nil {
					t.Fatal(err)
				}
				if _, verified, conflict := logging.IdentitySnapshot(ctx); verified || !conflict {
					t.Fatal("subsequent success cleared conflict")
				}

			} else {
				prior := logging.Identity{UserID: 99}
				logging.ObserveIdentity(ctx, prior)
				codec.Value(ctx, "Bearer "+value)
				if got, verified, conflict := logging.IdentitySnapshot(ctx); got != prior || !verified || conflict {
					t.Fatal("policy failure replaced earlier evidence")
				}
			}
			if candidate.allowed && (err != nil || claims == nil) {
				t.Fatalf("matching identity rejected: %v", err)
			}
			if !candidate.allowed && (err == nil || claims != nil) {
				t.Fatalf("mismatched identity accepted: %#v", claims)
			}
		})
	}
}
