package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xcodec "github.com/viant/xdatly/codec"
	"reflect"
	"strings"
	"testing"
)

func TestVerificationFailureCredentialRetention(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	const raw = "Bearer malformed-private-credential"
	for _, retain := range []bool{false, true} {
		t.Run(fmt.Sprint(retain), func(t *testing.T) {
			cfg := &Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "retention-test-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}, RetainFailedCredential: retain}
			service, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			cfg.RetainFailedCredential = !retain
			codec, err := service.New(&xcodec.Config{Body: JwtClaim, SourceType: reflect.TypeFor[string](), DestinationType: reflect.TypeFor[*jwt.Claims]()})
			if err != nil {
				t.Fatal(err)
			}
			if !VerifiesJWT(codec) {
				t.Fatal("native codec authority changed")
			}
			_, err = codec.Value(context.Background(), raw)
			var failure *VerificationFailure
			if !errors.As(err, &failure) {
				t.Fatalf("typed failure missing: %v", err)
			}
			got, ok := failure.FailedCredential()
			if ok != retain || (retain && got != raw) || (!retain && got != "") {
				t.Fatal("retention policy mismatch")
			}
			if errors.Unwrap(err) == nil {
				t.Fatal("verifier cause lost")
			}
			for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
				if strings.Contains(fmt.Sprintf(verb, err), "malformed-private-credential") {
					t.Fatal("credential formatting leak")
				}
			}
			encoded, e := json.Marshal(err)
			if e != nil || string(encoded) != "{}" {
				t.Fatalf("JSON exposure: %s %v", encoded, e)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = codec.Value(ctx, raw)
			if !errors.Is(err, context.Canceled) || errors.As(err, &failure) {
				t.Fatal("cancellation incorrectly retained")
			}
			for _, invalid := range []any{42, ""} {
				_, err = codec.Value(context.Background(), invalid)
				if err == nil || errors.As(err, &failure) {
					t.Fatal("non-verification failure changed")
				}
			}
		})
	}
}
