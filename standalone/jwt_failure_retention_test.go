package standalone

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xcodec "github.com/viant/xdatly/codec"
)

func TestStandaloneJWTFailureRetentionOptIn(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	for _, retain := range []bool{false, true} {
		t.Run(fmt.Sprint(retain), func(t *testing.T) {
			host := &source{config: &config.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "standalone-retention-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}, JWTRetainFailedCredential: retain}}
			if _, err := host.init(context.Background(), nil); err != nil {
				t.Fatal(err)
			}
			codec, err := host.codecs.New(&xcodec.Config{Body: auth.JwtClaim, SourceType: reflect.TypeFor[string](), DestinationType: reflect.TypeFor[*jwt.Claims]()})
			if err != nil {
				t.Fatal(err)
			}
			if !auth.VerifiesJWT(codec) {
				t.Fatal("standalone native verifier identity lost")
			}
			const credential = "Bearer standalone-private-invalid-credential"
			_, err = codec.Value(context.Background(), credential)
			var failure *auth.VerificationFailure
			if !errors.As(err, &failure) {
				t.Fatal("typed verification failure missing")
			}
			got, retained := failure.FailedCredential()
			if retained != retain || (retain && got != credential) || (!retain && got != "") {
				t.Fatal("trusted host retention policy mismatch")
			}
			if strings.Contains(fmt.Sprintf("%#v", failure), credential) {
				t.Fatal("formatted credential exposure")
			}
			encoded, marshalErr := json.Marshal(failure)
			if marshalErr != nil || string(encoded) != "{}" {
				t.Fatal("serialized credential exposure")
			}
		})
	}
}

func TestStandaloneJWTFailureRetentionConfiguration(t *testing.T) {
	for name, payload := range map[string]string{
		"default.json": `{}`,
		"optin.json":   `{"JWTRetainFailedCredential":true}`,
		"default.yaml": `{}`,
		"optin.yaml":   "JWTRetainFailedCredential: true\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := (config.Loader{}).Load(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			want := strings.HasPrefix(name, "optin")
			if cfg.JWTRetainFailedCredential != want {
				t.Fatal("loaded trusted policy differs")
			}
			resolved, err := cfg.ResolveConstants()
			if err != nil {
				t.Fatal(err)
			}
			if resolved.JWTRetainFailedCredential != want {
				t.Fatal("resolved trusted policy differs")
			}
		})
	}
}
