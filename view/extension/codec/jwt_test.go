package codec

import (
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
)

func TestJwtCacheIsScopedByVerifier(t *testing.T) {
	first := verifier.New(nil)
	second := verifier.New(nil)
	claims := &jwt.Claims{RegisteredClaims: jwtv5.RegisteredClaims{
		ExpiresAt: jwtv5.NewNumericDate(time.Now().Add(time.Hour)),
	}}
	cache := &JwtCache{entries: map[jwtCacheKey]*JwtEntry{}}
	cache.put("token", first, claims)
	if actual := cache.lookup("token", first); actual != claims {
		t.Fatalf("same-verifier lookup=%p want=%p", actual, claims)
	}
	if actual := cache.lookup("token", second); actual != nil {
		t.Fatalf("cross-verifier lookup=%p", actual)
	}
	if actual := cache.lookup("token", nil); actual != nil {
		t.Fatalf("GCP fallback lookup reused configured-verifier entry: %p", actual)
	}
}
