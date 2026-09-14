// Package auth supplies local RSA/JWT fixtures using the actual Datly verifier.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	rauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
)

type JWT struct {
	Config  *verifier.Config
	Factory *rauth.Service
	key     *rsa.PrivateKey
}

func NewJWT(t testing.TB) *JWT {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	config := &verifier.Config{RSA: []*scy.Resource{{URL: "local-test-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})}}}
	factory, err := rauth.New(context.Background(), &rauth.Config{JWTValidator: config})
	if err != nil {
		t.Fatal(err)
	}
	return &JWT{Config: config, Factory: factory, key: key}
}
func (j *JWT) Sign(t testing.TB, expiry time.Time) string {
	t.Helper()
	token := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "approved", "exp": expiry.Unix()})
	value, err := token.SignedString(j.key)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
