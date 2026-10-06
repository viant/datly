package auth

import (
	"bytes"
	"context"
	"testing"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/scy"
)

func TestEmbeddedWarmupRSAAssetsAreScyEncrypted(t *testing.T) {
	if _, err := jwtv5.ParseRSAPrivateKeyFromPEM(warmupPrivateEncrypted); err == nil {
		t.Fatal("embedded private key is readable without Scy decryption")
	}
	if _, err := jwtv5.ParseRSAPublicKeyFromPEM(warmupPublicEncrypted); err == nil {
		t.Fatal("embedded public key is readable without Scy decryption")
	}
	loader := scy.New()
	privateSecret, err := loader.Load(context.Background(), &scy.Resource{URL: "warmup-private-test", Data: append([]byte(nil), warmupPrivateEncrypted...), Key: "blowfish://default"})
	if err != nil {
		t.Fatal(err)
	}
	privatePEM := []byte(privateSecret.String())
	publicSecret, err := loader.Load(context.Background(), &scy.Resource{URL: "warmup-public-test", Data: append([]byte(nil), warmupPublicEncrypted...), Key: "blowfish://default"})
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := []byte(publicSecret.String())
	if bytes.Equal(privatePEM, warmupPrivateEncrypted) || bytes.Equal(publicPEM, warmupPublicEncrypted) {
		t.Fatal("embedded RSA asset was not decrypted")
	}
	private, err := jwtv5.ParseRSAPrivateKeyFromPEM(privatePEM)
	if err != nil {
		t.Fatal(err)
	}
	public, err := jwtv5.ParseRSAPublicKeyFromPEM(publicPEM)
	if err != nil {
		t.Fatal(err)
	}
	if !private.PublicKey.Equal(public) {
		t.Fatal("embedded RSA key pair does not match")
	}
}
