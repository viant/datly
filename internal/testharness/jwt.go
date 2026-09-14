package testharness

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
)

// JWT supplies local signed credentials and verifier resources without a TCP server.
type JWT struct {
	key    *rsa.PrivateKey
	public []byte
}

func NewJWT(t testing.TB) *JWT {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return &JWT{key: key, public: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}
}
func (j *JWT) Config() *verifier.Config {
	return &verifier.Config{RSA: []*scy.Resource{{URL: "test-public-key", Data: append([]byte(nil), j.public...)}}}
}
func (j *JWT) Sign(t testing.TB, subject string, expires time.Time) string {
	t.Helper()
	token := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": subject, "exp": expires.Unix()})
	signed, err := token.SignedString(j.key)
	if err != nil {
		t.Fatal(err)
	}
	return "Bearer " + signed
}
